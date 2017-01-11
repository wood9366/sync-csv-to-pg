#!/usr/local/bin/perl

# use warnings;
use strict;
use DBI;
use JSON;
use Data::Dumper;

sub load_config {
    my $path = shift;

    open my $fh, "<", $path or die "load config file [$path] fail, $!\n";

    my $json = JSON->new->allow_nonref;
    my $config = $json->decode(join("", <$fh>));

    close $fh;

    return $config;
}

my $configpath = shift @ARGV;
my $host_idx = shift @ARGV || 0;

my $config = &load_config($configpath);
my $host = $config->{hosts}[$host_idx];

if ($host) {
    my $dbh = DBI->connect("DBI:Pg:dbname=$host->{database};host=$host->{host};port=$host->{port}", $host->{username}, $host->{password}, {PrintError => 0, RaiseError => 1});

    if ($dbh) {
        my $config = { "hosts" => $config->{hosts} };

        my $sth = $dbh->table_info('', 'public', '%', 'TABLE');
        my @tables = map { $_->{TABLE_NAME} } @{$sth->fetchall_arrayref({TABLE_NAME => 1})};
        # print Dumper(\@tables);

        foreach my $table (@tables) {
            my $config_table = { name => $table };

            my $sth = $dbh->column_info('', 'public', $table, '%');
            my @columns = @{$sth->fetchall_arrayref({COLUMN_NAME => 1, TYPE_NAME => 1})};
            # print Dumper(\@columns);

            foreach my $col (@columns) {
                push @{$config_table->{fields}}, { name => $col->{COLUMN_NAME}, type => $col->{TYPE_NAME} };
            }

            push @{$config->{tables}}, $config_table;
        }

        $dbh->disconnect;

        my $json = JSON->new->allow_nonref->canonical;

        open my $fh, ">", "generated_config.json" or die "generate config fail, $!\n";

        print $fh $json->pretty->encode($config);

        close $fh;
    } else {
        print "error> create database connection fail, $!\n";
    }
} else {
    print "error> invalid host\n";
}

