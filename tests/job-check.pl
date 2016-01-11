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

my($opt, $usage) = describe_options("%c %o id",
				    ["shock_service_url=s" => "Shock URL", { default => 'https://ci.kbase.us/services/shock-api/' }],
				    ["handle_service_url=s" => "Handle service url", { default => 'https://ci.kbase.us/services/handle_service/' }],
				    ["ujs_service_url=s" => "UJS url", { default => 'https://ci.kbase.us/services/userandjobstate/'}],
				    ["transform_service_url=s" => "Transform service url", { default => 'https://ci.kbase.us/services/transform/'}],
				    ["workspace=s" => "Workspace name", { default => 'olson:1451943504644'}],
				    ["help|h" => 'Show this help message'],
				    );

print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV != 1;

my $id = shift;
				    
my $token;

my($user) = $token =~ /un=([^|]+)/;

$ENV{KB_AUTH_TOKEN} = $token;

my $ujs = Bio::KBase::userandjobstate::Client->new($opt->ujs_service_url);

my @res = $ujs->get_job_status($id);
print Dumper(\@res);
