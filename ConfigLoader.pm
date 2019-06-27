package BeaconPlus::ConfigLoader;

use File::Basename;
use YAML::XS qw(LoadFile);

use BeaconPlus::QueryParameters;

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
  my $query			=		BeaconPlus::QueryParameters->new();
  my $self      =   LoadFile(File::Basename::dirname( eval { ( caller() )[1] } ).'/config/config.yaml') or die print 'Content-type: text'."\n\n¡No config.yaml file in this path!";
  bless $self, $class;
  if ($ENV{SERVER_NAME} =~ /\.test$|\//) { $self->{url_base} =~  s/\.org/.test/ }
  # $self->{query}		=		$query;
  $self->{param}		=		$query->{param};
  $self->{queries}	=		$query->{queries};
  $self->{filters}  =   $query->{filters};
  $self->{scopes}   =   $query->{config}->{scopes};
  $self->{api_methods}  =   $query->{config}->{api_methods};
  $self->{query_errors}	=		$query->{query_errors};
  $self->{pretty_params}=   $query->{pretty_params};
  $self->{cgi}			=		$query->{cgi};

  $self->_select_dataset_from_param();

  return $self;

}

################################################################################

sub _select_dataset_from_param {

	my $config		=		shift;

	if (! grep{ /.../ } @{ $config->{param}->{datasetIds} } ) { return $config }

	my @datasets;
	foreach my $qds (@{ $config->{param}->{datasetIds} }) {
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
