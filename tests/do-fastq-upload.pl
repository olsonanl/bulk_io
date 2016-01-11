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

my($opt, $usage) = describe_options("%c %o filename1 filename2 ws-name",
				    ["n-uploads=i" => 'Number of times to upload the file'],
				    ["block-size=i" => "Size of blocks to use for uploads", { default = 2097152 }
				    ["parallel=i" => "Parallel threads", { default => 1 }],
				    ["shock_service_url=s" => "Shock URL", { default => 'https://ci.kbase.us/services/shock-api/' }],
				    ["handle_service_url=s" => "Handle service url", { default => 'https://ci.kbase.us/services/handle_service/' }],
				    ["ujs_service_url=s" => "UJS url", { default => 'https://ci.kbase.us/services/userandjobstate/'}],
				    ["transform_service_url=s" => "Transform service url", { default => 'https://ci.kbase.us/services/transform/'}],
				    ["workspace=s" => "Workspace name", { default => 'olson:1451943504644'}],
				    ["help|h" => 'Show this help message'],
				    );

print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV != 3;

my $file1_to_upload = shift;
my $file2_to_upload = shift;
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
	push(@work, [$file1_to_upload, $file2_to_upload, $ws, $i]);
    }
    pareach \@work, sub {
	my($work) = @_;
	my($f1, $f2, $n, $i) = @$work;
	upload_file($f1, $f2, $n, $i);
    }, { Max_Workers => $opt->parallel };
}
else
{
    upload_file($file1_to_upload, $file2_to_upload, $ws_name_base);
}

sub upload_file
{
    my($path1, $path2, $ws_name, $idx) = @_;

    my $t1 = gettimeofday;
    
    my($node1, $ujs1) = upload_one_file($path1, $idx);

    my $t2 = gettimeofday;
    printf STDERR "$path1\t$path2\tupload_time-1\t%f\n", $t2 - $t1;

    my($node2, $ujs2) = upload_one_file($path2, $idx);

    my $t3 = gettimeofday;
    printf STDERR "$path1\t$path2\tupload_time-2\t%f\n", $t3 - $t2;

    my $params = {
	external_type => 'SequenceReads',
	kbase_type => 'KBaseAssembly.PairedEndLibrary',
	workspace_name => $ws,
	object_name => $ws_name,
	optional_arguments => {
	    validate => {},
	    transform => {
		output_file_name => "pelib.fast1.json",
		outward => 0,
	    },
	},
	url_mapping => {
	    'SequenceReads.1' => "$shock_url/node/$node1",
	    'SequenceReads.2' => "$shock_url/node/$node2",
	},
    };
    my $tres = $transform->upload($params);
    
    my($awe_id, $job_id) = @$tres;
    print "Transforming: awe=$awe_id job=$job_id\n";

    my $t4 = gettimeofday;
    printf STDERR "$path1\t$path2\t$ws_name\ttransform_start\t%f\n", $t4 - $t3;

    my $last_status = "awaiting_start";
    my $last_t = $t4;
    
    while (1)
    {
	my @res = $ujs->get_job_status($job_id);
	
	my($last_update, $stage, $status, $progress, $est_complete, $complete, $error) = @res;

	if ($status ne $last_status)
	{
	    my $t = gettimeofday;
	    printf "$path1\t$path2\t$ws_name\tstatus=$last_status\t%f\n", $t - $last_t;
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

    my $t5 = gettimeofday;
    printf STDERR "$path1\t$path2\t$ws_name\ttransform\t%f\n", $t5 - $t4;
    printf STDERR "$path1\t$path2\t$ws_name\ttotal\t%f\n", $t5 - $t1;
}

sub upload_one_file
{
    my($path, $idx) = @_;
        
    my @stat = stat($path);
    @stat or die "File $path does not exist\n";
    my $mod_time = $stat[9] * 1000 + $idx;
    my $size = $stat[7];

    my $nres = $ua->post("$shock_url/node", @auth_header);
    
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

    local $HTTP::Request::Common::DYNAMIC_FILE_UPLOAD = 1;
    
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
    
    my $ujs_key = join(":", "File", $size, $mod_time, basename($path), $user);

    my $now = int(gettimeofday * 1000);
    
    print "key=$ujs_key now=$now\n";
    
    $ujs->set_state("ShockUploader", $ujs_key, "$node_id $now");

    return($node_id, $ujs_key);
}
