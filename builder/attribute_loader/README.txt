
Process attribute data into generic db. Uses sqlite as an intermediate 
representation (unlike the scripts in the dbutil module that invent an 
ad-hoc in memory set of table-like objects using dictionaries). Functionality
divided into smallish modules, to be used in the following order.

 * process_identifiers: loads processed identifier mappings
 
 * process_attributes: defines an attribute group and loads its corresponding 
   attributes. For now these are extracted from the same file that contains
   the gene-attribute associations, but in the future we'll want separate metadata
   files that include names & descriptions. Internal ids are assigned in this step
   for attribute groups and attributes.
   
 * process_attribute_associations: load the given gene-attribute association data,
   processing against known identifiers and outputing an association file mapped
   to internal id's suitable for loading into the engine backend
   
 * export_attributes: populates generic_db with attribute metadata files

Attribute group ids and attribute ids are unique at the organism level. 

A separate sqlite db should be created per oranism, named by the genemania
organism id. 

The processing modules include unit-tests, but not main methods as they are intended
to be used from a control script. A rough driver script is in process_all, and the
attribute_processor module in dbutil serves as the main production driver script, 
controlled by properties configured in the master db.cfg. 

