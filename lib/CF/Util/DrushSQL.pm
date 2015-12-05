package CF::Util::DrushSQL;
use 5.022000;
use Text::CSV;
use namespace::autoclean;
use Moose;

our $VERSION = '0.01';

has 'drush' => ( is => 'rw', isa => 'Str' );
has 'drupal_root' => ( is => 'rw', isa => 'Str' );
has 'csv' => ( is => 'rw', isa => 'Text::CSV', init_arg => undef);

sub get_queries {
    my $field_list = "(entity_type, bundle, deleted, entity_id, revision_id, language, delta, %s)";
    return {
        sql_list_vocabularies => "SELECT vid, machine_name FROM taxonomy_vocabulary ORDER BY machine_name;",
          sql_get_terms_in_vocabulary => "SELECT tid, name FROM taxonomy_term_data WHERE vid = %d ORDER BY name;",
          sql_insert_field_data_field => "INSERT INTO %s $field_list VALUES ('%s', '%s', %d, %d, %d, 'und', %d, %d);",
      sql_field_data_field_dupe_check => "SELECT count(*) FROM %s WHERE entity_type = '%s' AND entity_id = %d AND revision_id = %d AND %s = %d;",
      sql_insert_field_revision_field => "INSERT INTO %s $field_list VALUES ('%s', '%s', %d, %d, %d, 'und', %d, %d);",
  sql_field_revision_field_dupe_check => "SELECT count(*) FROM %s WHERE entity_type = '%s' AND entity_id = %d AND revision_id = %d AND %s = %d;",
                   sql_update_history => "UPDATE history SET timestamp = %d WHERE nid = %d AND uid = 1;",
                      sql_update_node => "UPDATE node SET changed = %d WHERE vid = %d;",
             sql_update_node_revision => "UPDATE node_revision SET timestamp = %d WHERE vid = %d;",
            sql_insert_taxonomy_index => "INSERT INTO taxonomy_index (nid, tid, sticky, created) VALUES (%d,%d,%d,(SELECT created FROM node WHERE nid = %d));",
            sql_update_taxonomy_index => "UPDATE taxonomy_index SET created = %d WHERE nid = %d;",
        sql_taxonomy_index_dupe_check => "SELECT count(*) FROM taxonomy_index WHERE nid = %d AND tid = %d",
         sql_get_term_id_from_node_id => "SELECT tid FROM taxonomy_index WHERE nid = %d;",
    sql_get_vocabulary_name_from_tvid => "SELECT machine_name FROM taxonomy_vocabulary WHERE vid = %d;",
               sql_get_node_meta_data => "SELECT vid, type, created, title FROM node WHERE nid = %d;",
   sql_get_vocabulary_id_from_term_id => "SELECT vid FROM taxonomy_term_data WHERE tid = %d",
                sql_begin_transaction => "BEGIN;",
               sql_commit_transaction => "COMMIT;",
             sql_rollback_transaction => "ROLLBACK;",
    }
}
sub query {
    my($self,$sql) = @_;
    my @rows;
    my $command = $self->drush() . " " . qq(sqlq "$sql") . " -r " . $self->drupal_root();
    my $sql_result = `$command`;
    open my ($str_fh), '<', \$sql_result;
    while ( my $row = $self->get_csv()->getline( $str_fh ) ) {
        if(scalar @{$row} = 1 ? $row->[1] ne '' : scalar @{$row} > 1) {
            push @rows, $row;
        }
    }
#    my @results = `$command`;
#    chomp(@results);
#    // sep_char \t
    close $str_fh;
    return @rows;
}
sub get_node_meta_data_sql {
    my($self,$nid) = @_;
    return sprintf $self->get_queries()->{'sql_get_node_meta_data'}, $nid;
}
sub get_csv {
    my($self) = @_;
    if($self->csv) {
        return $self->csv();
    } else {
        $self->csv(Text::CSV->new ({ binary => 1, eol => $/, sep_char => '\t' }));
        return $self->csv();
    }
}

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

CF::Util::DrushSQL - Perl extension for blah blah blah

=head1 SYNOPSIS

use CF::Util::DrushSQL;
blah blah blah

=head1 DESCRIPTION

Stub documentation for CF::Util::DrushSQL, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

James Jones, E<lt>james@localE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2015 by James Jones

This library is free software;
you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.22.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
