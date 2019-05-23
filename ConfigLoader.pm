package BeaconPlus::ConfigLoader;

use CGI::Simple;
use File::Basename;
use YAML::XS qw(LoadFile);

require Exporter;
@ISA    =   qw(Exporter);
@EXPORT =   qw(
  new
  _dw
);

sub new {

=pod

=cut

  my $class     =   shift;  
  my $self      =   LoadFile(File::Basename::dirname( eval { ( caller() )[1] } ).'/config/config.yaml') or die print 'Content-type: text'."\n\n¡No config.yaml file in this path!";
  bless $self, $class;
  if ($ENV{SERVER_NAME} =~ /\.test$|\//) { $self->{url_base} =~  s/\.org/.test/ }
  
  $self->select_dataset_from_param();

  return $self;

}

################################################################################

sub select_dataset_from_param {

	my $config		=		shift;

	my @datasets;
	my $cgi    		=  CGI::Simple->new;

	foreach my $qds ($cgi->param('datasetIds')) {
		if (grep{ $qds eq $_ } @{ $config->{dataset_names} }) {
			push(@datasets, $qds) }	
	}

	if (@datasets > 0) {
		$config->{dataset_names}	=		\@datasets }

	return $config;

}

################################################################################

sub _dw {
  use Data::Dumper;
	print	'Content-type: text/html'."\n\n";
  print Dumper(@_);
}

################################################################################

1;
