package BeaconPlus::ConfigLoader;

use File::Basename;
use YAML::XS qw(LoadFile);

use BeaconPlus::QueryParameters;

require Exporter;
@ISA            =   qw(Exporter);
@EXPORT         =   qw(
	new
	RandArr
	_dw
);

sub new {

=podmd
The BeaconPlus::ConfigLoader library

* configures the execution environment based on the parameters in the `./config/config.yaml` file
* calls the BeaconPlus::QueryParameters library to create the query objects

Objects accessible through `$config`

* `$config->{cgi}`
    - provides access to CGI parameters
* `$config->{param}`
    - deparsed CGI parameters, where all have been treated as arrays
* `$config->{queries}`
    - the pre-defined query objects for the different query scopes
        * `$config->{queries}->{variants}` contains the query object against the "variants" collection
* `$config->{filters}`
    - filters for e.g. query result post-processing (e.g. "randno")
* `$config->{query_errors}`


=cut

  my $class     =   shift;
  my $self      =   LoadFile(File::Basename::dirname( eval { ( caller() )[1] } ).'/config/config.yaml') or die print 'Content-type: text'."\n\n¡No config.yaml file in this path!";
  bless $self, $class;

  if ($ENV{SERVER_NAME} =~ /\.test$|\//) { $self->{url_base} =~  s/\.org/.test/ }
  my $query		=		BeaconPlus::QueryParameters->new();
  
  my $ho		=	LoadFile(File::Basename::dirname( eval { ( caller() )[1] } ).'/config/handover_types.yaml') or die print 'Content-type: text'."\n\n¡No handover_types.yaml file in this path!";
  $self->{handover_types}	=	$ho->{handover_types};

  my $ds		=	LoadFile(File::Basename::dirname( eval { ( caller() )[1] } ).'/config/datasets.yaml') or die print 'Content-type: text'."\n\n¡No datasets.yaml file in this path!";
  $self->{datasets}		=	$ds->{datasets};

  $self->{q_conf}		=	$query->{config};
  $self->{param}		=	$query->{param};
  $self->{queries}		=	$query->{queries};
  $self->{filters}  	=   $query->{filters};
  $self->{api_methods}  =   $query->{config}->{api_methods};
  $self->{query_errors}	=	$query->{query_errors};
  $self->{cgi}			=	$query->{cgi};
  $self->{debug}		=	$query->{debug};

  $self->_select_datasets_from_param();

  return $self;

}

################################################################################

sub _select_datasets_from_param {

	my $config		=		shift;

	$config->{dataset_names}	=		[ map{ $_->{id} } @{ $config->{datasets} } ];

  ##############################################################################
  
  my %dsParams  =   map{ $_ => 1 } @{ $config->{param}->{datasetIds} }, @{ $config->{param}->{apidb} };

	if (! grep{ /.../ } keys %dsParams ) { return $config }

=podmd

Only datasets from the query are used if specified as "datasetIds".

=cut
  
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

sub RandArr {

=podmd
### RandArr

This dependency-free array randomiser will return a re-shuffled array(ref) or
a slice of random $iL array elements.

The $overSamp factor is "empirical" and balances between oversampling with
out-of-range values + filtering, and cycling too many times to match all
index elements.

#### Expects:

* an array reference of arbitrary content
* the number of array elements to be returned (optional)

#### Return

* the re-shuffled array or a subset of its elements (as array reference)

=cut

  my $arr       =   shift;
  my $iL        =   shift;
  my $overSamp  =   7;

  if (ref $arr ne 'ARRAY') { return \0 }

  # if no number of array elements => all
  if ($iL !~ /^\d+?$/) {
    $iL = scalar @$arr }
  # ... not more than all
  elsif ($iL > @$arr) {
    $iL = scalar @$arr }

  $overSamp *= $iL;

  # maximum index number, for filtering the oversampled values
  my $maxI      =   @$arr - 1;
  if ($maxI < 0) { return \0 }

  # use of a hash to have unique index numbers (keys of the hash)
  my %randNo    =   ();

  # adding to the hash keys until equal or more than needed
  while (keys %randNo < $iL) {
    %randNo     =   map{ $_ => 1 } (grep{ $_ <= $maxI } (keys %randNo, map{ int(rand($_)) } 0..$overSamp) );
  }

  return [ @$arr[ (keys %randNo)[0..($iL-1)] ] ];

}
################################################################################

sub _dw {
  use Data::Dumper;
	print	'Content-type: text/html'."\n\n";
  print Dumper(@_);
}

################################################################################

1;
