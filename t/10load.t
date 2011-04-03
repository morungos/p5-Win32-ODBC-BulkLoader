# Test the adler32 implementation I stole from libxdiff.

use strict;
use warnings;
use Test::More tests => 2;

BEGIN { use_ok( 'Win32::ODBC::BulkLoader' ); }

#my $result = Win32::ODBC::BulkLoader::load("driver={SQL Server};Server=localhost;Database=load_sqlserver_a;Trusted_Connection=Yes",
#                                           "inventory_components",
#                                           q{C:\TEMP\Desjardins\ORA\20090908\data\load_root_inventory_components.tab},
#                                           q{C:\workspace\ARM\schemas\sql\sqlserver\inventory_components.fmt});
#ok($result);

my $result = Win32::ODBC::BulkLoader::load("driver={SQL Server};Server=192.168.0.145;Database=ibarm_test;Trusted_Connection=Yes",
                                           "relations",
                                           q{C:\TEMP\InformationBalance\Demo\20090908\data\load_MAINFRAME_relations.tab},
                                           q{C:\workspace\ARM\schemas\sql\sqlserver\relations.fmt}, 0);

ok($result);

1;
