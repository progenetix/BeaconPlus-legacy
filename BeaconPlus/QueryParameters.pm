package BeaconPlus::QueryParameters;

use Data::Dumper;
use CGI::Simple;
use BeaconPlus::ConfigLoader;

require Exporter;
@ISA    =   qw(Exporter);
@EXPORT =   qw(
  new
  read_param_config
  convert_api_request
  map_scoped_params
  get_variant_params
  norm_variant_params
  check_variant_params
  create_variant_query
  create_subsets_queries
  create_handover_query
);

sub new {

  use File::Basename;

=podmd
The _BeaconPlus_ environment utilizes the _Beacon_ protocol for federated genomic
variant queries, extended by methods discussed in the Beacon API development
and custom extensions which may - or may not - make it into the Beacon
specification but help to increase the usability of the
[Progenetix](https://progenetix.org) resource.

=cut

  my $class     =   shift;

  my $self      =   {
    here_path       =>  File::Basename::dirname( eval { ( caller() )[1] } ),
    query_errors    =>  {},
    parameters      =>  {},
    queries         =>  {},
    cgi             =>  CGI::Simple->new,
  };

  bless $self, $class;
  
  $self->{debug}	=		-1;
  
	# the debug print is done here so that it can be used for query debugging...
  if ($self->{cgi}->param(debug) > 0) {
  	$self->{debug}	=		1;
    print 'Content-type: text/plain'."\n\n";
  }

#  $self->read_beacon_specs();
  $self->read_param_config();
  $self->read_filter_mappings();
  $self->deparse_query_string();
  $self->convert_api_request();
  $self->scope_filters();
  $self->map_scoped_params();
  $self->norm_variant_params();
  $self->check_variant_params();
  $self->create_variant_query();
  $self->create_sample_queries();
  $self->create_subsets_queries();
  $self->create_handover_query();

  return $self;

}

################################################################################

sub read_beacon_specs {

=podmd
### Reading the Beacon Specification

While the specification _in principle_ follows the Beacon specification, and
offers a minimal method to access it, this optioned isn't used in practice due
to the "forward looking" nature of some of the BeaconPlus methods.

=cut

  use YAML::XS qw(LoadFile);

  my $query     =   shift;
  $query->{beacon_spec} =   LoadFile($query->{here_path}.'/specification/beacon.yaml');
  return $query;

}

################################################################################

sub read_param_config {

  use YAML::XS qw(LoadFile);

  my $query     =   shift;
  $query->{config}  =   LoadFile($query->{here_path}.'/config/query_params.yaml');
  return $query;

}

################################################################################

sub read_filter_mappings {

  use YAML::XS qw(LoadFile);

  my $query     =   shift;
  my $filter_mappings		=		LoadFile($query->{here_path}.'/config/filter_mappings.yaml');
  $query->{filter_mappings}  =   $filter_mappings->{parameters};
  return $query;

}

################################################################################

sub deparse_query_string {

=podmd
#### Deparsing the query string

The query string is deparsed into a hash reference, in the "$query" object,
with the conventions of:

* each parameter is treated as containing a list of values
* values are __split into a list by the comma character__; so an example of
    `key=val1&key=val2,val3`
  would be deparsed to
    `key = [val1, val2, val3]`

The treatment of each attribute as pointing to an array leads to a consistent,
though sometimes awkward, access to the values; even consistently unary values
have to be addressed as (first) array element.

=cut

  my $query     =   shift;

  foreach my $qkey ($query->{cgi}->param()) {

    my @qvalues =   $query->{cgi}->param($qkey);
    if ($qkey =~ /\w/ && grep{ /./ }  @qvalues) {

      foreach my $val (grep{ /./} split(',', join(',', @qvalues))) {
        push(@{ $query->{param}->{$qkey} }, $val);
      }
    }
  }

  return $query;

}

################################################################################

sub convert_api_request {

=podmd
An API request is converted in two stages:

1. API shortcuts are resolved; i.e. requests requiring a specific database
and/or collection may have pre-defined `api_shortcuts` values to allow the use
of simple canonical URIs, which at this stage are being expanded to the full
format.

2. The API request is split & mapped to standard query parameters.

=cut

  my $query     =   shift;

  my $uri				=		$ENV{REQUEST_URI};

  if ($uri !~ /^\/api\//) { return $query }

  foreach my $short (grep{ $uri =~ $_ } keys %{ $query->{config}->{api_shortcuts} }) {
  	$uri 				=~	s/\/api\/$short\//$query->{config}->{api_shortcuts}->{$short}/ }

  my @request   =   grep{ /./ } split('/', $uri);

  if ($request[0] ne 'api') { return $query }

  shift @request;  # remove the api part
  foreach (@{ $query->{config}->{api_mappings} }) {
    $query->{param}->{ $_->{paramkey} } =     [ $_->{default} ];
    if ($request[0] =~ /^\?/i)  { last }
    if ($request[0] !~ /\w/i)   { shift @request; next }
    $query->{param}->{ $_->{paramkey} } =  [ split(',', shift @request) ];
  }

  return $query;

}


################################################################################

sub scope_filters {

  my $query     =   shift;

# 	HACK: EFO filter for Beacon if not specified, to avoid timeout errors...
# 	if ($ENV{SCRIPT_NAME} =~ /beacon/) {	
# 		if (! grep{/EFO/} @{ $query->{param}->{filters} }) {
# 			push(@{ $query->{param}->{filters} }, 'EFO:0009656') } }
		
  foreach my $filterV  (@{ $query->{param}->{filters} }) {
    foreach my $pre  (keys %{ $query->{filter_mappings} }) {
      if ($filterV  =~   /^$pre(\:|\-|$)/) {

        if ($query->{filter_mappings}->{$pre}->{remove_prefix}) {
          $filterV  =~  s/^$pre(\:|\-)?// }
        push(
          @{ $query->{param}->{ $query->{filter_mappings}->{$pre}->{parameter} } },
          $filterV
        );
  }}}

  return $query;

}

################################################################################

sub map_scoped_params {

=podmd
#### Matching parameters to their scopes

In the configuration file, the root attribute `scopes` contains the definitions
of the different "scopes" (essentially the different data collections) and which
query parameters can be applied to them. These definitions also provide

* `alias` values
    - allowing to use different names for the parameter e.g. in forms (avoiding
    dot annotation problems)
* `paramkey`
    - the fully expanded database attribute, including the collection name
        * `publications.provenance.geo.city`
* `dbkey`
    - the attribute, w/o prepended collection

Foreach of the scopes, the pre-defined _possible_ parameters are evaluated for
corresponding values in the object generated from parsing the query string. If
matching values are found those are added to the pre-formatted query parameter
object for the corresponding scope. Those scoped parameter objects will then be
processed depending on the type of query (e.g. "variants" queries have a
different processing compared to "biosamples" queries; see below).

=cut

  my $query     =   shift;

  foreach my $scope (keys %{ $query->{config}->{scopes} }) {
    my $thisP   =   $query->{config}->{scopes}->{$scope}->{parameters};
    foreach my $q_param (grep{ /\w/ } keys %{ $thisP }) {
      my $dbK   =   $thisP->{$q_param}->{dbkey} =~ /\w/ ? $thisP->{$q_param}->{dbkey} : $q_param;
      foreach my $alias ($q_param, $thisP->{$q_param}->{paramkey}, $thisP->{$q_param}->{dbkey}, @{ $thisP->{$q_param}->{alias} }) {
      foreach my $val (@{ $query->{param}->{$alias} }) {
         if ($thisP->{$q_param}->{type} =~/(?:num)|(?:int)|(?:float)/i) {
            $val  =~  tr/[^\d\.\-]//;
            if (grep{ $q_param =~ /$_/ } qw(start end)) { $val =~ s/[^\d]//g }
            $val  *=  1;
          }
          if ($val =~ /./) {
            if ($val =~ /$thisP->{$q_param}->{pattern}/) {
              if ($thisP->{$q_param}->{type} =~ /array/i) {
              	if (! grep{ $val =~ /^$_$/ } @{ $query->{parameters}->{$scope}->{$dbK} }) {
                	push(@{ $query->{parameters}->{$scope}->{$dbK} }, $val)
              }}
              else {
                $query->{parameters}->{$scope}->{$dbK}  =   $val;
  }}}}}}}
  
  $query->{queries}->{filters}	=		$query->{parameters}->{filters};

  return $query;

}

################################################################################

sub norm_variant_params {

  my $query     =   shift;

=podmd
#### Normalization of variant query parameters

The `norm_variant_params` function creates intervals for variant 
("BeaconAlleleRequest") queries from interpolation of all "start" and "end" 
parameters. This is done greedily, i.e. allowing for incorrect submission order 
and mix of e.g. "startMax" and "startMin" parameter types. The decision if a query 
with such a mix should be rejected is handled elsewhere.

The output of the routine are:

* `start_range`
    - 2-value array for start matches
    - interbase format (i.e. if only one value was provided the range will be
    [value, value + 1]
* `end_range`
    - as above
* `pos_range`
    - 2-value array from start to end
    - e.g. for range matches
    
##### Interpolation of "SNV" type

If `variantType: "SNV"` is specified w/o `alternateBases` value, the wildcard 
"N" value is inserted.

=cut

  my @rangeVals =   ();

  foreach my $side (qw(start end)) {
    my $parKeys =   [ grep{ /^$side(?:_m(?:(?:in)|(?:ax)))?$/ } keys %{ $query->{parameters}->{variants} } ];
    my @parVals =   grep{ /^\d+?$/ } @{ $query->{parameters}->{variants} }{ @$parKeys };
    if (@parVals > 0) {
      if (@parVals == 1) { push(@parVals, ($parVals[0] + 1)) }
      @parVals    =   sort {$a <=> $b} @parVals;
      if ($parVals[0] == $parVals[-1]) { $parVals[-1]++ }
      $query->{parameters}->{variants}->{$side.'_range'}  =  [ $parVals[0], $parVals[-1] ];
      push(@rangeVals, $parVals[0], $parVals[-1]);
    }
  }

  @rangeVals    =  sort {$a <=> $b} grep{  /^\d+?$/ } @rangeVals;
  $query->{parameters}->{variants}->{pos_range} =   [ $rangeVals[0], $rangeVals[-1] ];
  
  
  if (
    $query->{parameters}->{variants}->{alternate_bases} !~ /^\w+?$/
    &&
    $query->{parameters}->{variants}->{variant_type} =~ /^SN[VP]$/
  ) {
    $query->{parameters}->{variants}->{alternate_bases} =   'N' }

  $query->{parameters}->{variants}->{reference_name}    =~  s/chr?o?//i;

  return $query;

}

################################################################################

sub check_variant_params {

  my $query     =   shift;
  my $varPars   =   $query->{parameters}->{variants};
  my $varParRef =   $query->{config}->{scopes}->{variants}->{parameters};

  # TODO: Use the Beacon specification for allowed values
  
  $query->{query_errors}->{variants}  =   [];
 
  if (! grep{ $varPars->{$_} =~ /\w/ } keys %$varPars) {
  	$query->{query_errors}->{variants}  =   ['ERROR: Empty variant query'] }
  
  # check existence of assemblyId
  if ($varPars->{assembly_id} !~  /$varParRef->{assemblyId}->{pattern}/) {
    push(
      @{ $query->{query_errors}->{variants} },
      'ERROR: Missing correct "assemblyId" value'
    );
  }

  # check existence of reference name
  if ($varPars->{reference_name} !~  /$varParRef->{referenceName}->{pattern}/) {
    push(
      @{ $query->{query_errors}->{variants} },
      'ERROR: Missing correct "referenceName" value ("'.$query->{parameters}->{variants}->{reference_name}.'")'
    );
  }

  # check erroneous concurrent use of "start" and "startMin" ("startMax")
  if (
    $query->{param}->{start}->[0] =~  /^\d+?$/
    &&
    ($query->{param}->{startMin}->[0] =~ /^\d+?$/ || $query->{param}->{startMax}->[0] =~ /^\d+?$/ )
  ) {
    push(
      @{ $query->{query_errors}->{variants} },
      'ERROR: concurrent use of "start" and "startMin" (and/or "startMax")'
    );
  }

  # check erroneous concurrent use of "end" and "endMin" ("endMax")
  if (
    $varPars->{end} =~  /^\d+?$/
    &&
    ( $varPars->{end_min}->[0] =~ /^\d+?$/ || $varPars->{end_max}->[0] =~ /^\d+?$/ )
  ) {
    push(
      @{ $query->{query_errors}->{variants} },
      'ERROR: concurrent use of "end" and "endMin" (and/or "endMax")'
    );
  }

  # check existing start range
  # those can either be provided through start (end) or startMin + startMax (…)
  # parameters since the single values will be converted to [val, val+1] ranges
  if (
    $varPars->{variant_type} =~ /^(DUP|DEL|BND)$/
    &&
    ( $varPars->{start_range}->[0] !~ /^\d+?$/ )
  ) {
    push(
      @{ $query->{query_errors}->{variants} },
      'ERROR: "start" (and also startMin, startMax) did not contain a numeric value.'
    );
  }
# print Dumper($varPars->{start_range}, $varPars->{end_range});
  if (
    $varPars->{variant_type} !~ /$varParRef->{variantType}->{pattern}/
    &&
    $varPars->{reference_bases} !~ /^[ATGCN]+?$/
  ) {
    push(
      @{ $query->{query_errors}->{variants} },
      'ERROR: There was no valid value for either "referenceBases or variantType".'
    );
  }

  if (
    $varPars->{variant_type} !~ /$varParRef->{variantType}->{pattern}/
    &&
    $varPars->{alternate_bases} !~ /^[ATGCN]+?$/
  ) {
    push(
      @{ $query->{query_errors}->{variants} },
      'ERROR: There was no valid value for either "alternateBases or variantType".'
    );
  }
#BeaconPlus::ConfigLoader::_dw($varParRef->{variantType}->{pattern}, $varParRef->{referenceName}->{pattern}, $varPars->{variant_type}, $query->{query_errors}->{variants});
  return $query;

}

################################################################################

sub create_variant_query {

  my $query     =   shift;
  my $varPars   =   $query->{parameters}->{variants};
  my $varParRef =   $query->{config}->{scopes}->{variants}->{parameters};

  if ($varPars->{variant_type} =~ /^DUP|DEL|CNV$/i){
  	if ($varPars->{end_range}->[1] > 0) {
    	$query->create_cnv_query() }
    else {
    	$query->create_cnv_point_query() }
  }
  elsif ($varPars->{variant_type} =~ /$varParRef->{variantType}->{pattern}/i) {
    $query->create_vartype_query() }
  elsif ($varPars->{alternate_bases} =~ /$varParRef->{alternateBases}->{pattern}/) {
    $query->create_precise_query() }
# print Dumper($query->{queries}->{variants});
  return $query;

}

################################################################################

sub create_cnv_query {

  my $query     =   shift;

  $query->{queries}->{variants} =   {
    '$and'    => [
      { reference_name      =>  $query->{parameters}->{variants}->{reference_name} },
      { start_max =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[0] } },
      { end_max   =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{end_range}->[0] } },
      { start_min =>  { '$lt'   =>  1 * $query->{parameters}->{variants}->{start_range}->[1] } },
      { end_min   =>  { '$lt'   =>  1 * $query->{parameters}->{variants}->{end_range}->[1] } },
      { variant_type        =>  $query->{parameters}->{variants}->{variant_type} },
    ],
  };

  return $query;

}

################################################################################

sub create_cnv_point_query {

=podmd

#### CNV "Point" Query

This query type is based on the assumption that a query consisting of

* a CNV variant type
    - `DUP`
    - `DEL`
    - `CNV` (i.e. either `DUP` or `DEL`)
* a single `start` parameter (or `startMin` and `startMax`)
* _no_ `end` parameter (also no `endMin` and `endMax`)

... aims to detect any CNV of the given type overlapping the `start` position.

```
---------s-----------------------

------+++++++++++++++++++++++++++
++++++++++-----------------------
-------+++++++-------------------
```

=cut

  my $query     =   shift;
  
  my $varTq			=		{ variant_type => $query->{parameters}->{variants}->{variant_type} };
  if ($query->{parameters}->{variants}->{variant_type} eq 'CNV') {
  	$varTq			=		{ 
  		'$or' =>  [
        { variant_type  =>  'DUP' },
        { variant_type  =>  'DEL' },
        { variant_type  =>  'CNV' },
      ]
    };
  }
  
  $query->{queries}->{variants}	=   { 
  	'$and' => 	[
			{ reference_name  =>  $query->{parameters}->{variants}->{reference_name} },
			$varTq,
			{ start_min =>  { '$lt'  =>  1 * $query->{parameters}->{variants}->{start_range}->[1] } },
			{ end_max =>  	{ '$gt'  =>  1 * $query->{parameters}->{variants}->{start_range}->[0] } },
		]
	};
  
  return $query;

}
################################################################################

sub create_bnd_query {

  my $query     =   shift;

  $query->{queries}->{variants} =   {
    '$and'      => [
      { reference_name  =>  $query->{parameters}->{variants}->{reference_name} },
      { '$or' =>  [
        { variant_type  =>  'DUP' },
        { variant_type  =>  'DEL' },
        { variant_type  =>  'BND' },
        { variant_type  =>  'CNV' },
      ] },
      { '$or'   =>  [
        { '$and'=> [
            { start_max =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[0] } },
            { start_min =>  { '$lt'  =>  1 * $query->{parameters}->{variants}->{start_range}->[1] } },
          ]
        },
        { '$and'=> [
            { end_max   =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[0] } },
            { end_min   =>  { '$lt'  =>  1 * $query->{parameters}->{variants}->{start_range}->[1] } },
          ]
        },
      ] },
    ],
  };

  return $query;

}

################################################################################

sub create_vartype_query {

  my $query     =   shift;

  $query->{queries}->{variants} =   {
    '$and'      => [
      { reference_name  =>  $query->{parameters}->{variants}->{reference_name} },
      { variant_type    =>  $query->{parameters}->{variants}->{variant_type} },
      { start_max =>  { '$gte'  =>  1 * $query->{parameters}->{variants}->{start_range}->[0] } },
      { start_min =>  { '$lt'  =>  1 * $query->{parameters}->{variants}->{start_range}->[1] } },
    ],
  };
  
  return $query;

}

################################################################################

sub create_precise_query {

=podmd
#### Function `create_precise_query`

This function handles the generation of the variant query for "precise" variants
(i.e. such annotated with  `referenceBases` and `alternateBases`, but including
wildcard matches).

TODO: Split-off of the truly precise queries with single start` positional 
parameter

=cut

  my $query     =   shift;

  foreach (qw(reference_bases alternate_bases)) {
    if ($query->{parameters}->{variants}->{$_} =~ /N/) {
      $query->{parameters}->{variants}->{$_} =~  s/N/./g;
      $query->{parameters}->{variants}->{$_} =   qr/^$query->{parameters}->{variants}->{$_}$/;
    }
  }

  my @qList     =   (
    { reference_name  =>  $query->{parameters}->{variants}->{reference_name} },
    { alternate_bases =>  $query->{parameters}->{variants}->{alternate_bases} },
  );

  if ($query->{param}->{start}->[0] =~ /^\d+?$/) {
    push(
      @qList,
      { start_min => 1 * $query->{param}->{start}->[0] },
    );
  } else {
    push(
      @qList,
      { start_max =>  { '$gt'  =>  1 * $query->{parameters}->{variants}->{pos_range}->[0] } },
      { start_min =>  { '$lt'  =>  1 * $query->{parameters}->{variants}->{pos_range}->[-1] } },
    );  
  }

  if ($query->{parameters}->{variants}->{reference_bases} =~ /^[ATCG\.]+?$/) {
    push(
      @qList,
      { reference_bases =>  $query->{parameters}->{variants}->{reference_bases} },
    );
  }

  $query->{queries}->{variants} =   { '$and' => \@qList };
  return $query;

}

################################################################################

sub format_geo_query {

	my $qList			=		shift;
	
	my $geoQ			=		{};
	my %geoPars		=		();
	
	my $cleaned		=		[];
		
	foreach my $qItem (@{$qList}) {
		my ($key, $value)	=		%{$qItem};
		if ($key =~ /geojson/) {
			$geoPars{$key}	=		$value }
		else {
			push(@$cleaned, $qItem) }
	}
	
	if (
		$geoPars{'provenance.geo.geojson.lat'}	=~ /\d/
		&&
		$geoPars{'provenance.geo.geojson.long'}	=~ /\d/
		&&
		$geoPars{'provenance.geo.geojson.maxdist'}	=~ /\d/
	) {
		$geoQ				=   {
			"provenance.geo.geojson"   =>  {
				'$near'     =>  {
					'$geometry'   =>  {
						"type"      		=>  "Point",
						"coordinates"   => [
							1 * $geoPars{'provenance.geo.geojson.long'},
							1 * $geoPars{'provenance.geo.geojson.lat'},
						],
					},
					'$maxDistance'    => $geoPars{'provenance.geo.geojson.maxdist'},
				}
			}
		};
		push(@$cleaned, $geoQ);		
	}

	return $cleaned;

}



################################################################################


sub create_sample_queries {

=podmd

#### Sample (_biosamples_ and _callsets_) Queries

Queries with multiple options for the same attribute are treated as logical "OR".
=cut

  my $query     =   shift;

  foreach my $scope (qw(biosamples callsets)) {
  
  	my @numKeys	=		();
  	my $thisP   =   $query->{config}->{scopes}->{$scope}->{parameters};
    foreach my $q_param (grep{ /\w/ } keys %{ $thisP }) {
      my $dbK   =   $thisP->{$q_param}->{dbkey} =~ /\w/ ? $thisP->{$q_param}->{dbkey} : $q_param;
      if ($thisP->{$q_param}->{type} =~/(?:num)|(?:int)|(?:float)/i) {
    		push(@numKeys, $dbK) }    
    }

    my @qList;

    foreach my $qKey (keys %{ $query->{parameters}->{$scope} }) {

      my %thisQlist;
      if (ref $query->{parameters}->{$scope}->{$qKey} eq 'ARRAY') {
        foreach (@{ $query->{parameters}->{$scope}->{$qKey} }) {
        	if (! grep{ $qKey eq $_ } @numKeys) {
          	$thisQlist{ $qKey.'::'.$_ }  =    { $qKey => qr/^$_/i } }
          else {
          	$thisQlist{ $qKey.'::'.$_ }  =    { $qKey => $_ } }
        }
      }
      else {
      	if (! grep{ $qKey eq $_ } @numKeys) {
        	$thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => qr/^$query->{parameters}->{$scope}->{$qKey}/i } }
        else {
        	$thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => $query->{parameters}->{$scope}->{$qKey} } }        
      }

      if (scalar keys %thisQlist == 1)    { push(@qList, (values %thisQlist)[0]) }
      elsif (scalar keys %thisQlist > 1)  {

=podmd
#### Special Boolean handling

The automatic Boolean query logic follows:

* query values of the same scope are treated as *OR*
* different scopes are connected trough *AND*

For `biocharacteristics` there is one exception: Query values for `icdom` and 
`icdot` are connected with *AND*, even though they target the same scope. This 
is due to the assumption that one may want to subset samples of a given 
morphology by topography (and _vice versa_), and that ICD-O M + T also can be
mapped to single ontologies like NCIt.


The current code just looks for the co-existence of `icdom` and `icdot` prefixed
values & then constructs some fancy "$or" and "$and" request to MongoDB.

=cut

      	if ($qKey eq "biocharacteristics.type.id") {
					if (
						(grep{ $_ =~ /icdom/ } keys %thisQlist)
						&&
						(grep{ $_ =~ /icdot/ } keys %thisQlist)
					) {
						my @bioQlist;
						my $icdQlist	=		[];
						foreach my $qKey (grep{ $_ !~ /icdo[mt]/} keys %thisQlist) {
							push(@bioQlist, $thisQlist{$qKey}) }
						foreach my $pre (qw(icdom icdot)) {
							my @iList;
							foreach my $iKey (grep{ $_ =~ /$pre/} keys %thisQlist) {
								push(@iList, $thisQlist{$iKey});							
							}
							if (@iList == 1) {
								push(@$icdQlist, $iList[0]) }
							else {
								push(@$icdQlist, {'$or' => [ @iList ] } );						
							}
						}
						push(@bioQlist, {'$and' => $icdQlist } );	
      			push(@qList, {'$or' => [ @bioQlist ] } );
      		} else {
      			push(@qList, {'$or' => [ values %thisQlist ] } ) }      		
      	} else {
      		push(@qList, {'$or' => [ values %thisQlist ] } ) }
      }
    }

=podmd

The construction of the query object depends on the detected parameters:

* if empty list => no change, empty object
* if 1 parameter => direct use
* if several parameters are queried => connection through the MongoDB  "$and" constructor

=cut

    @qList			=		@{ format_geo_query(\@qList) };

    if (@qList == 1)    { $query->{queries}->{$scope} =   $qList[0] }
    elsif (@qList > 1)  { $query->{queries}->{$scope} =   { '$and' => \@qList } }
    
  }
  return $query;

}

################################################################################

sub create_subsets_queries {

  my $query     =   shift;

  foreach my $scope (qw(biosubsets datacollections publications)) {

  	my @numKeys	=		();
  	my $thisP   =   $query->{config}->{scopes}->{$scope}->{parameters};
    foreach my $q_param (grep{ /\w/ } keys %{ $thisP }) {
      my $dbK   =   $thisP->{$q_param}->{dbkey} =~ /\w/ ? $thisP->{$q_param}->{dbkey} : $q_param;
      if ($thisP->{$q_param}->{type} =~/(?:num)|(?:int)|(?:float)/i) {
    		push(@numKeys, $dbK) }    
    }

    my @qList;

    foreach my $qKey (keys %{ $query->{parameters}->{$scope} }) {
      my %thisQlist;

      if (ref $query->{parameters}->{$scope}->{$qKey} eq 'ARRAY') {
        foreach my $val (@{ $query->{parameters}->{$scope}->{$qKey} }) {        
        	if (! grep{ $qKey eq $_ } @numKeys) {
          	$thisQlist{ $qKey.'::'.$val }  =    { $qKey => qr/^$val/i } }
          else {
          	$thisQlist{ $qKey.'::'.$val }  =    { $qKey => $val } }
        }
      }
      else {

        my $val =  $query->{parameters}->{$scope}->{$qKey};

        if ($val =~ /^(<|>\=?)(\d+?(\.\d+?)?)$/) {
          my ($rel, $num) 	=    ($1, 1 * $2);
          if ($rel eq '>') {
            $thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => { '$gt' => $num } } }
          if ($rel eq '<') {
            $thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => { '$lt' => $num } } }
          if ($rel eq '>=') {
            $thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => { '$gte' => $num } } }
          if ($rel eq '<=') {
            $thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => { '$lte' => $num } } }
        } else {
					if (! grep{ $qKey eq $_ } @numKeys) {
						$thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => qr/^$val/i } }
					else {
						$thisQlist{ $qKey.'::'.$query->{parameters}->{$scope}->{$qKey} }  = { $qKey => $val } }
				}
      }
      if (scalar keys %thisQlist == 1)    { push(@qList, (values %thisQlist)[0]) }
      elsif (scalar keys %thisQlist > 1)  { push(@qList, {'$or' => [ values %thisQlist ] } ) }
    }

    @qList			=		@{ format_geo_query(\@qList) };

    if (@qList == 1)    { $query->{queries}->{$scope} =   $qList[0] }
    elsif (@qList > 1)  { $query->{queries}->{$scope} =   { '$and' => \@qList } }

  }

  return $query;

}

################################################################################

sub create_handover_query {

  my $query     =   shift;
  if (! $query->{parameters}->{handover}->{id}) { return $query }
  if ($query->{parameters}->{handover}->{id} !~ /$query->{config}->{scopes}->{handover}->{parameters}->{accessid}->{pattern}/) { return $query }

  $query->{queries}->{handover} =   { id  =>  $query->{parameters}->{handover}->{id} };

  return $query;

}

################################################################################

1;
