use Data::Dumper;
use strict;
use HTTP::Request::Common;
use Bio::KBase::userandjobstate::Client;
use Bio::KBase::Transform::Client;
use JSON::XS;
use LWP::UserAgent;
use File::Basename;
use Getopt::Long::Descriptive;
use Time::HiRes 'gettimeofday';
use Proc::ParallelLoop;

my($opt, $usage) = describe_options("%c %o filename ws-name",
				    ["n-uploads=i" => 'Number of times to upload the file'],
				    ["parallel=i" => "Parallel threads", { default => 1 }],
				    ["shock_service_url=s" => "Shock URL", { default => 'https://ci.kbase.us/services/shock-api/' }],
				    ["handle_service_url=s" => "Handle service url", { default => 'https://ci.kbase.us/services/handle_service/' }],
				    ["ujs_service_url=s" => "UJS url", { default => 'https://ci.kbase.us/services/userandjobstate/'}],
				    ["transform_service_url=s" => "Transform service url", { default => 'https://ci.kbase.us/services/transform/'}],
				    ["workspace=s" => "Workspace name", { default => 'olson:1451943504644'}],
				    ["help|h" => 'Show this help message'],
				    );

print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV != 2;

my $file_to_upload = shift;
my $ws_name_base = shift;				    

# --shock_service_url https://ci.kbase.us/services/shock-api/ --handle_service_url https://ci.kbase.us/services/handle_service/ --ujs_service_url https://ci.kbase.us/services/userandjobstate/

my $token;

my($user) = $token =~ /un=([^|]+)/;

$ENV{KB_AUTH_TOKEN} = $token;

my $ua = LWP::UserAgent->new;
my $json = JSON::XS->new->pretty(1);

my $shock_url = $opt->shock_service_url;
my $ws = $opt->workspace;
my $ujs = Bio::KBase::userandjobstate::Client->new($opt->ujs_service_url);
my $transform  = Bio::KBase::Transform::Client->new($opt->transform_service_url);

my $stats = $ujs->list_state("ShockUploader", 0);
print Dumper($stats);

my @auth_header = ("Authorization"  => "OAuth $token");

if ($opt->n_uploads)
{
    my @work;
    for (my $i = 0; $i < $opt->n_uploads; $i++)
    {
	my $ws = sprintf "$ws_name_base-%04d", $i;
	push(@work, [$file_to_upload, $ws, $i]);
    }
    pareach \@work, sub {
	my($work) = @_;
	my($f, $n, $i) = @$work;
	upload_file($f, $n, $i);
    }, { Max_Workers => $opt->parallel };
}
else
{
    upload_file($file_to_upload, $ws_name_base);
}

sub upload_file
{
    my($path, $ws_name, $idx) = @_;

    my @stat = stat($path);
    @stat or die "File $path does not exist\n";
    my $mod_time = $stat[9] * 1000 + $idx;
    my $size = $stat[7];

    my $t1 = gettimeofday;

    my $nres = $ua->post("$shock_url/node",
			 @auth_header);
    
    if (!$nres->is_success)
    {
	die "shock post failed: " . $nres->status_line . " " . $nres->content . "\n";
    }
    
    my $ndata = $json->decode($nres->content);
    
    if ($ndata->{status} ne 200)
    {
	die "Bad status from shock node request\n" . Dumper($ndata);
    }
    my $node_id = $ndata->{data}->{id};
    print "Got node id $node_id\n";
    
    my $req = HTTP::Request::Common::POST("$shock_url/node/$node_id",
					  @auth_header,
					  Content_Type => 'multipart/form-data',
					  Content => [upload => [$path]]);
    $req->method('PUT');
    
    my $upres = $ua->request($req);
    if (!$upres->is_success)
    {
	die "Upload put failed: " . $upres->message . " " . $upres->content . "\n";
    }
    my $updata = $json->decode($upres->content);
    
    my $ujs_key = join(":", "File", $size, $mod_time, $ws_name, $user);

    my $t2 = gettimeofday;
    printf STDERR "$path\t$ws_name\tupload_time\t%f\n", $t2 - $t1;
    
    my $now = int(gettimeofday * 1000);
    
    print "key=$ujs_key now=$now\n";
    
    my $res = $ujs->set_state("ShockUploader", $ujs_key, "$node_id $now");
    print "Set: " . Dumper($res);
    
    my $params = {
	external_type => 'FASTA.DNA.Assembly',
	kbase_type => 'KBaseGenomes.ContigSet',
	workspace_name => $ws,
	object_name => $ws_name,
	optional_arguments => {
	    validate => {},
	    transform => {
		fasta_reference_only => 'false',
	    },
	},
	url_mapping => {
	    'FASTA.DNA.Assembly' => "$shock_url/node/$node_id",
	},
    };
    my $tres = $transform->upload($params);
    
    my($awe_id, $job_id) = @$tres;
    print "Transforming: awe=$awe_id job=$job_id\n";

    my $t3 = gettimeofday;
    printf STDERR "$path\t$ws_name\ttransform_start\t%f\n", $t3 - $t2;

    my $last_status = "awaiting_start";
    my $last_t = $t3;
    
    while (1)
    {
	my @res = $ujs->get_job_status($job_id);
	
	my($last_update, $stage, $status, $progress, $est_complete, $complete, $error) = @res;

	if ($status ne $last_status)
	{
	    my $t = gettimeofday;
	    printf "$path\t$ws_name\tstatus=$last_status\t%f\n", $t - $last_t;
	    $last_t = $t;
	    $last_status = $status;
	}
	
	if ($complete)
	{
	    print "Complete: status=$status\n";
	    last;
	}
	sleep 1;
    }

    my $t4 = gettimeofday;
    printf STDERR "$path\t$ws_name\ttransform\t%f\n", $t4 - $t3;
    printf STDERR "$path\t$ws_name\ttotal\t%f\n", $t4 - $t1;
}
