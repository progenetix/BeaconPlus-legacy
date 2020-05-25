#!/usr/bin/perl

# © 2000-2019 Michael Baudis: m@baud.is

use strict;
use CGI::Carp qw(fatalsToBrowser);
use CGI qw(param);

use File::Basename;
use JSON;
use MongoDB;
use MongoDB::MongoClient;
use Data::Dumper;

use BeaconPlus::ConfigLoader;

=podmd
The "beaconinfo.cgi" server application serves tw main functions

1. Called w/o parameters, it will return a standard Beacon information object.
2. Provided with
    - `datasetIds` (optional)
    - `querytext`
    - `querytype` (optional)
it will return information about the matched identifiers from the collection(s)
selected through `datasetIds`.

=cut

my $config      =   BeaconPlus::ConfigLoader->new();
my $autoc       =   $config->{param}->{autocomplete}->[0];

if (! -t STDIN) { print 'Status: 200'."\n".'Content-type: application/json'."\n\n" }

if ($ENV{REQUEST_URI} =~ /service\-info/) {
  print JSON::XS->new->pretty( 1 )->allow_blessed->convert_blessed->encode($config->{service_info})."\n";
  exit;
}

=podmd
The counts for the collections (`variants`, `biosamples` etc.) of the different
datasets are retrieved from the daily updated `progenetix.dbstats` collection.

For the non-parametrized call of the application, just the basic information
including variant counts is returned.

=cut

my $dbClient    =   MongoDB::MongoClient->new();
my $cursor			=		$dbClient->get_database( 'progenetix' )->get_collection('dbstats')->find()->sort({_id => -1})->limit(1);
my $stats				=		($cursor->all)[0];

my $beaconInfo	=		$config->{service_info};
foreach (qw(id name apiVersion version description welcomeUrl alternativeUrl createDateTime updateDateTime organization sampleAlleleRequests)) {
	$beaconInfo->{$_}	=		 $config->{$_};
}

foreach my $dataset ( @{ $config->{ datasets }}) {

	my $counts		=		{
		callCount				=>	$stats->{$dataset->{id}.'__variants'}->{count},
		variantCount		=>	$stats->{$dataset->{id}.'__variants'}->{distincts_count_digest},
		sampleCount			=>	$stats->{$dataset->{id}.'__biosamples'}->{count},
	};

  if (grep{ $counts->{$_} > 0 } keys %$counts) {

    my $dbconn    =   $dbClient->get_database( $dataset->{id} );

    foreach (sort keys %$counts) {
      if ($counts->{$_} >	0) {
        $dataset->{$_}	=		$counts->{$_} }
    }
  
=podmd

If the request contains an "ontologies" or "details" keyword, information about 
the existing ontologies are provided, per dataset.

TODO: This may either be deprecated (since an alternative exists in `/api/`),
or be specified more clearly in the future, depending also on the development
of the Beacon v2 "filters" concept.

=cut

    if ($ENV{REQUEST_URI}  =~ /details|ontolog/) {

      my $collName  	=   'biosubsets';
      if ($ENV{REQUEST_URI}  =~ /referenceid/) {
        $collName =   'datacollections' }

      my $cursor  =   $dbconn->get_collection($collName)->find( $config->{queries}->{biosamples} )->fields({ id => 1, label => 1, count => 1, _id => 0});
      my @subsets =   $cursor->all;
      foreach my $sub (grep{ $_->{id} !~ /\+/ } @subsets) {
        $sub->{count}		*=	1;	
        push(@{ $dataset->{ info }->{ ontology_terms } }, $sub);
      }
    }

    push(@{ $beaconInfo->{datasets} }, $dataset);

  }
}

print JSON::XS->new->pretty( 1 )->allow_blessed->convert_blessed->canonical()->encode($beaconInfo)."\n";

exit;

1;
