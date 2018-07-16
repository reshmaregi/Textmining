-- use kb_pubmed;

-- truncate table kb_pubmed.pmc_filelist;
delete from kb_pubmed.pmc_filelist;

load data local infile '/sc/orga/projects/PBG/KBase/download/ftp.ncbi.nlm.nih.gov/pub/pmc/file_list.txt'
into table pmc_filelist
fields terminated by '\t'
lines terminated by '\n'
ignore 1 lines
(filename, citation, pmcid, @pmid)
set pmid = if(@pmid like "PMID:%", cast(substr(@pmid from 6) as unsigned), null)
;

show warnings;

load data local infile '/sc/orga/projects/PBG/KBase/download/ftp.ncbi.nlm.nih.gov/pub/pmc/file_list.pdf.txt'
into table pmc_filelist
fields terminated by '\t'
lines terminated by '\n'
ignore 1 lines
(filename, citation, pmcid, @pmid)
set pmid = if(@pmid like "PMID:%", cast(substr(@pmid from 6) as unsigned), null)
;

show warnings;
