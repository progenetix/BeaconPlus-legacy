package BeaconPlus::QueryExecution;

use Data::Dumper;
use UUID::Tiny;
use BeaconPlus::ConfigLoader;

require Exporter;
@ISA    =   qw(Exporter);
@EXPORT =   qw(
  new
  prefetch_data
  create_handover_object
  execute_aggregate_query
);

sub new {

  use File::Basename;
  $MongoDB::Cursor::timeout = 120000;

=podmd
This module provides the complete query execution, result aggregation and
creation of temporary storage objects for the BeaconPlus infrastructure.

It assumes a MongoDB document database system, with one or several databases
running separate collections corresponding to the main entities in the
[GA4GH core data model](https://schemablocks.org/categories/formats/ga4gh-data-model.html):

* variants
* callsets
* biosamples
* individuals

Additionally, the system uses a `handover` collection for temporary storage of
matched documents (`_id` values and query metadata).

For each of the collections 0 - 1 query objects may be provided through the
`$config->{queries}` object and accessed e.g. through
`$prefetch->{queries}->{biosamples}` etc.

### Query Aggregation

After performing an optional _handover_ query (which may populate one of the
other queries), queries for all 4 scopes are checked & executed if existing.

At each stage a match value < 1 will lead to exit from the query.

=cut

  my $class     =   shift;
  my $config    =   shift;
  my $dataset   =   shift;

  my $self      =   {
    dataset			=>	$dataset,
    queries     =>  $config->{queries},
    filters     =>  $config->{filters},
    db_conn			=>	MongoDB::MongoClient->new()->get_database( $dataset ),
    handover_coll   =>   MongoDB::MongoClient->new()->get_database( $config->{handover_db} )->get_collection( $config->{handover_coll} ),
  };

  bless $self, $class;
  return $self;

}

################################################################################

sub execute_aggregate_query {

  my $prefetch  =   shift;

=podmd

#### 1. __handover__ query

If a _handover_ query is provided (e.g. a different set of query parameters has
been used in a previous query and now a subset of those results should be
retrieved), this query is executed first. The documents from this query are then
added as requirement to the corresponding query; e.g. if the h->o query returned
a scope of "biosamples" and a list of "id" values, the
`$prefetch->{queries}->{biosamples}` query will be extended with an

```
{ '$and' => { 'id' => { '$in' => $handover->{target_values} } } }
```

More documentation can be found [below](#prefetch_to_scoped_query).

=cut

  $prefetch->prefetch_to_scoped_query();
  $prefetch->get_base_counts();

  if (! grep{ /../ }
    keys %{ $prefetch->{queries}->{biosamples} },
    keys %{ $prefetch->{queries}->{callsets} },
    keys %{ $prefetch->{queries}->{variants} },
    keys %{ $prefetch->{queries}->{individuals} }
  ) { return }

=podmd

#### 2. __callsets__ query

If a specific _callsets_ query exists, those ids are retrieved first and stored
in a handover object `$prefetch->{handover}->{'callsets::id'}`.

=cut

  # the main prefetch method here is the retrieval of the
  # "callsets" collection's "id" values
  my $method    =   'callsets::id';

  if (grep{ /../ } keys %{ $prefetch->{queries}->{callsets} } ) {

    my $thisM   =   'callsets::id';

    $prefetch->prefetch_data( $thisM, $prefetch->{queries}->{callsets} );
    $prefetch->{counts}->{callsets_query_match_count}  =   $prefetch->{handover}->{$thisM}->{target_count};
    if ($prefetch->{handover}->{$thisM}->{target_count} < 1) {
      return $prefetch }

  }

=podmd
#### 3. __biosamples__ query

If a specific __biosamples__ query exists, their ids are stored ina a _handover_
object `$prefetch->{handover}->{'biosamples::id'}`.

=cut

  if (grep{ /../ } keys %{ $prefetch->{queries}->{biosamples} } ) {

    my $thisM   =   'biosamples::id';

    $prefetch->prefetch_data( $thisM, $prefetch->{queries}->{biosamples} );

    $prefetch->{counts}->{biosamples_query_match_count}  =   $prefetch->{handover}->{$thisM}->{target_count};
    if ($prefetch->{handover}->{$thisM}->{target_count} < 1) {
      return $prefetch }

=podmd
##### 3a. callsets aggregation on matched biosamples

If biosamples were matched, the corresponding `callset_id` values are retrieved.
If already a 'callsets::id' h->o object exists, the queries are combined using
intersection through added "$in" query construct
=cut

    my $thisQ   =   { 'biosample_id' => { '$in' =>  $prefetch->{handover}->{'biosamples::id'}->{target_values} } };

    if ($prefetch->{handover}->{'callsets::id'}->{target_count} > 0) {
      $thisQ    =   { '$and' =>
        [
          $thisQ,
          { $prefetch->{handover}->{'callsets::id'}->{target_key} => { '$in' =>  $prefetch->{handover}->{'callsets::id'}->{target_values} } },
        ],
      };
    }

    $prefetch->prefetch_data( 'callsets::id', $thisQ );

    if ($prefetch->{handover}->{'callsets::id'}->{target_count} < 1) {
      return $prefetch }
  }

=podmd
#### 4. __variants__ query

A variant query is checked & executed if defined. If there had been _callsets_
matches before they will be intersected through an added "$in" query.

The results of the variant query are stored in the `variants::_id` h->o objects
(there is no `id` attribute in variants and the internal `_id` attribute is
preferable if no referencing is needed).

The results of the variant are used to re-query the _variants_ collection for
refreshing the `callsets::id` values to the matched `variants.callset_id`
values.
=cut

  if (grep{ /../ } keys %{ $prefetch->{queries}->{variants} } ) {

    my $thisQ   =   $prefetch->{queries}->{variants};

    if ($prefetch->{handover}->{'callsets::id'}->{target_count} > 0) {

      $thisQ    =   { '$and' =>
        [
          $thisQ,
           { 'callset_id' => { '$in' =>  $prefetch->{handover}->{'callsets::id'}->{target_values} } },
        ],
      };

    }

    $prefetch->create_handover_object( 'variants::_id', $thisQ );

    if ($prefetch->{handover}->{'variants::_id'}->{target_count} < 1) {
      return $prefetch }

    $thisQ      =   { '_id' => { '$in' => $prefetch->{handover}->{'variants::_id'}->{target_values} } };
    $thisM      =   'variants::callset_id';
    $prefetch->prefetch_data( $thisM, $thisQ );

    # just to overwrite the key with the standard 'callsets::id' results
    $prefetch->{handover}->{'callsets::id'}->{target_values}  =   $prefetch->{handover}->{$thisM}->{target_values};
    $prefetch->{handover}->{'callsets::id'}->{target_count}  =   $prefetch->{handover}->{$thisM}->{target_count};

    # getting the distinct variants
    $thisQ      =   { '_id' => { '$in' => $prefetch->{handover}->{'variants::_id'}->{target_values} } };
    $thisM      =   'variants::digest';
    $prefetch->prefetch_data( $thisM, $thisQ );

  }

=podmd
#### 5. Final result aggregation and storage of the `_id` based h->o objects

Up to this point, queries against callsets, biosamples and variants have
been reduced to the `callsets::id` values since those can be used for linking
from variants and to biosamples.

Those `callsets::id` results are now used to generate the reference handover
objects for callsets and biosamples, and to store them in the handover
collection.

* fetch the `biosample_id` values from the matched callsets
* query the _biosamples_ collection for those values and store the
corresponding biosample pointers using `biosamples::_id`
* re-query the _callsets_ collection for the values from `callsets::id`
and store the corresponding pointers using `callsets::_id`

=cut

  $prefetch->prefetch_data(
    'callsets::biosample_id',
    { 'id' => { '$in' => $prefetch->{handover}->{'callsets::id'}->{target_values} } },
  );

  $prefetch->create_handover_object(
    'biosamples::_id',
    { 'id' => { '$in' => $prefetch->{handover}->{'callsets::biosample_id'}->{target_values} } },
  );
  $prefetch->create_handover_object(
    'callsets::_id',
    { 'id' => { '$in' => $prefetch->{handover}->{'callsets::id'}->{target_values} } },
  );

=podmd
#### 5a. Populating the `counts` object

Foreach of callsets, biosamples and variants the object count is added to the
`$prefetch->{counts}` object, for use in the `BeaconResponse`:

* `$prefetch->{counts}->{callsets_match_count}`
* `$prefetch->{counts}->{biosamples_match_count}`
* `$prefetch->{counts}->{variants_match_count}`
    - optional; only if a variants query had been provided

=cut

  foreach (qw(callsets biosamples variants)) {
    if ($prefetch->{handover}->{$_.'::_id'}->{target_count}) {
      $prefetch->{counts}->{$_.'_match_count'} =   $prefetch->{handover}->{$_.'::_id'}->{target_count} }
  }

=podmd

### Query results

The `$prefetch` object now contains a set of of `handover` objects, which point
to all documents of a given domain which have fulfilled all criteria of the
different queries:

* `$prefetch->{handover}->{callsets::_id}`
* `$prefetch->{handover}->{biosamples::_id}`
* `$prefetch->{handover}->{variants::_id}`
    - optional; only if a variants query had been provided

=cut

  return $prefetch;

}

################################################################################

sub prefetch_data {

=podmd

### Handover objects

`handover` objects provide
* `source_db`
* `source_collection`
    - the queried collection which may be different from the target
        * query `biosample_id` values in "callsets" (source) which target
          `id` values in "biosamples" (target)
* `target_collection`
    - the database collection the values refer to
* `target_key`
    - the key (attribute) the values refer to
* `target_values`
    - the values which have to be matched for `target_key`
* `target_count`
    - count of `target_values`; convenience value

These objects are used internally for data aggregation purposes, but
can be stored in the temporary ("capped" in MongoDB) "handover" collection
for accessing e.g. through their unique key, e.g. in "Beacon handover"
scenarios.

=cut

  my $prefetch  =   shift;
  my $method    =   shift;
  my $this_Q    =   shift;
  my $filters   =   shift;

  my (
    $source_c,
    $source_k,
    $target_c,
    $target_k
  )             =   split('::', $method);

  # two components are interpolated to same output
  if ($target_c !~  /\w/) { $target_c = $source_c }
  if ($target_k !~  /\w/) { $target_k = $source_k }

  my $distincts =   $prefetch->{db_conn}->run_command([
                      "distinct"=>  $source_c,
                      "key"     =>  $source_k,
                      "query"   =>  $this_Q,
                    ]);

  my $distVals  =   $distincts->{values};
  if ($prefetch->{filters}->{randno} > 0) {
    $distVals   =   BeaconPlus::ConfigLoader::RandArr($distVals, $prefetch->{filters}->{randno}) }

  $prefetch->{handover}->{$method}  =   {
    source_db         =>  $prefetch->{dataset},
    source_collection =>  $source_c,
    source_key        =>  $source_k,
    target_collection =>  $target_c,
    target_key        =>  $target_k,
    target_values     =>  $distVals,
    target_count      =>  scalar @{ $distVals },
  };

  return $prefetch;

}

################################################################################

sub create_handover_object {

=podmd
#### Creating Handover documents

The `create_handover_object` method is a wrapper for the `prefetch_data`
function. It calls this function to execute a query and retrieve a handover
object, and then stores this object in the handover collection for later
retrieval by its unique (UUID v4) `_id`.

This anonymous `_id` can be safely exposed to external data retrieval systems.

=cut
  my $prefetch  =   shift;
  my $method    =   shift;
  my $this_Q    =   shift;

  $prefetch->prefetch_data($method, $this_Q);
  $prefetch->{handover}->{$method}->{_id} =   create_UUID_as_string();

  $prefetch->{handover_coll}->insert( $prefetch->{handover}->{$method} );

  return $prefetch;

}

################################################################################

sub get_base_counts {

  my $prefetch  =   shift;

  $prefetch->{filters}  ||= {};

  $prefetch->{counts}   =   {};
  foreach (qw(callsets biosamples variants)) {
    $prefetch->{counts}->{$_.'_base_count'}   =   $prefetch->{db_conn}->get_collection($_)->count();
  }
  return $prefetch;

}

################################################################################

sub prefetch_to_scoped_query {

=podmd

#### <a id="prefetch_to_scoped_query"></a>Extracting pre-selected items from a handover query

Handover objects are generally used to retrieve documents based on their stored
`_id` (or other key) values.

In the "collection name equals object model core entity" scenario used here,
the `prefetch_to_scoped_query` method directly creates a scoped query against
the target collection; i.e. if the `target_collection` value is "variants",
the variants query of the `$prefetch->{queries}` object will be created or
modified.

=cut

  my $prefetch  =   shift;
  if (! grep{ /../ } keys %{ $prefetch->{queries}->{handover} } ) { return $prefetch }

  my $handover  =   $prefetch->{handover_coll}->find_one( $prefetch->{queries}->{handover} );

  if (! $handover->{target_values}) { return $prefetch }
  my $h_o_q			=		{ $handover->{target_key} => { '$in' => $handover->{target_values} } };
  my $scope			=		$handover->{target_collection};

  if (grep{ /../ } keys %{ $prefetch->{queries}->{$scope} } ) {
  	$prefetch->{queries}->{$scope}	=		{ '$and' => [
  $h_o_q,
  $prefetch->{queries}->{$scope}
  ] };
  } else {
  	$prefetch->{queries}->{$scope}	=		$h_o_q }

  return $prefetch;

}


1;
