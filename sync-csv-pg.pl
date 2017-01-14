#!/usr/local/bin/perl

# use warnings;
use strict;
use DBI;
use File::Basename;
use File::Spec::Functions;
use JSON;
use Data::Dumper;

sub load_csv {
    my $path = shift;

    open(my $fh, "<", $path) or die "can't find csv $path\n";

    my $line_names = <$fh>;
    my $line_types = <$fh>;

    chomp $line_names;

    chomp $line_types;


    sub trim {
        $_ = shift;
        s/^\s*"//;
        s/"\s*$//;
        $_;
    }

    my @names = split /","/, $line_names;
    my @types = split /","/, $line_types;

    @names = map { &trim($_) } @names;
    @types = map { &trim($_) } @types;

    return if not @names;

    my @fields = map { { name => $names[$_], type => $types[$_] } } (0 .. $#names);

    foreach (@fields) {
        if ($_->{type} =~ /\[\]$/) {
            $_->{pgtype} = 'VARCHAR';
        } else {
            if ($_->{type} =~ /INT|FLAG|ENUM|BOOL/) {
                $_->{pgtype} = 'INT';
            } elsif ($_->{type} =~ /FLOAT/) {
                $_->{pgtype} = 'REAL';
            } elsif ($_->{type} =~ /STRING|EXPRESSION|CONDITION/) {
                $_->{pgtype} = 'VARCHAR';
            }
        }
    }

    my @rows = ();

    while (<$fh>) {
        chomp;
        push @rows, [map { &trim($_); } split /","/];
    }

    close $fh;

    return { fields => \@fields, rows => \@rows };
}

sub load_config {
    my $path = shift;

    open my $fh, "<", $path or die "load config file [$path] fail, $!\n";

    my $json = JSON->new->allow_nonref;
    my $config = $json->decode(join("", <$fh>));

    close $fh;

    return $config;
}

sub sync_table {
    my $dbh = shift;
    my $table = shift;
    my $csv = shift;

    # todo, check table exist and create table
    my $sth = $dbh->table_info('', 'public', "$table->{name}", 'TABLE');
    my @tables = @{$sth->fetchall_arrayref};

    if (@tables) {
        # todo, check all table fields match

        print "info> clean table [$table->{name}]\n";

        # clean data
        my $sql_cleantable = "DELETE FROM $table->{name}";

        # print "sql> $sql_cleantable\n";

        eval { $dbh->do($sql_cleantable); };
        if ($@) {
            print "clean table data fail, $@\n", "sql> $sql_cleantable\n";
            return 0;
        }
    } else {
        print "info> create table [$table->{name}]\n";

        my $sql_createtable = "CREATE TABLE $table->{name} "
            . "(" . join(",", map { "\"$_->{name}\" $_->{type}" } @{$table->{fields}}) . ")";

        # print "sql> $sql_createtable\n";

        eval { $dbh->do($sql_createtable); };
        if ($@) {
            print "error> create table fail, $@\n", "sql> $sql_createtable\n";
            return 0;
        }
    }

    # insert all data
    print "info> insert table [$table->{name}] content\n";
    my $sql_insert_rows = "INSERT INTO $table->{name}"
        . "(" . join(",", map { "\"$_->{name}\"" } @{$table->{fields}}) . ")"
        . " VALUES "
        . join(",", map {
            my $row = $_;

            "(" . join(",", map {
                my $item = $row->[$_];
                my $type = $table->{fields}[$_]{type};

                if ($type eq 'varchar') {
                    defined($item) ? "'$item'" : "''";
                } else {
                    defined($item) ? ($item + 0) : 0;
                }
            } (0..$#{$table->{fields}})) . ")";

            # use column map
            # "(" . join(",", map {
            #     my $field = $_;
            #     my $item = undef;

            #     my ($csv_field_idx) = grep { $csv->{fields}[$_]->{name} eq $field->{map} } (0..$#{$csv->{fields}});

            #     if ($csv_field_idx) {
            #         $item = $row->[$csv_field_idx];
            #     } else {
            #         print "warn> no match data map for column $field->{map}\n";
            #     }

            #     if ($item) {
            #         ($fields->{type} eq "integer") ? "'$item'" : $item;
            #     } else {
            #         ($fields->{type} eq "integer") ? "''" : 0;
            #     }
            # } @{$table->{fields}}) . ")";
        } @{$csv->{rows}});

    # print "sql> $sql_insert_rows\n";

    eval { $dbh->do($sql_insert_rows); };
    if ($@) {
        print "insert rows fail, $@\n", "sql> $sql_insert_rows\n";
        return 0;
    }

    return 1;
}

sub sync {
    my $host = shift;
    my $tables = shift;
    my $datadir = shift;

    print "info> sync to host $host->{host}:$host->{port}\n";

    my $dbh = DBI->connect("DBI:Pg:dbname=$host->{database};host=$host->{host};port=$host->{port}", $host->{username}, $host->{password}, {PrintError => 0, RaiseError => 1});

    if ($dbh) {
        foreach my $table (@$tables) {
            &sync_table($dbh, $table, &load_csv(catfile($datadir, $table->{data})));
        }

        $dbh->disconnect;
    } else {
        print "error> create database connection fail, $!\n";
    }

    print "info> done";
}

my $configpath = shift @ARGV;
my $datadir = shift @ARGV;

my $config = &load_config($configpath);
# print Dumper($config);

die "no host" if (scalar @{$config->{hosts}} < 0);

# choose sync host
my $choose_host_input = 0;

do {
    print "hosts:\n";
    foreach (0 .. $#{$config->{hosts}}) {
        my $h = $config->{hosts}[$_];
        print "$_ > $h->{host}:$h->{port} $h->{database}\n";
    }
    print "choose host: ";
    chomp($choose_host_input = <STDIN>);
} while(not $choose_host_input =~ /\d|\*/);


if ($choose_host_input eq '*') {
    foreach my $host (@{$config->{hosts}}) {
        &sync($host, $config->{tables}, $datadir);
    }
} else {
    my $host = $config->{hosts}[$choose_host_input];

    if ($host) {
        &sync($host, $config->{tables}, $datadir);
    } else {
        print "invalid host, [$choose_host_input]";
    }
}
