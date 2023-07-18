## Update loadSpec tool

Tool for updating loadSpec in Druid segment metadata required when migrating the DeepStorage. The segment metadata must
be exported in CSV format. The tool automatically update the payload column of every row according to the target stored.

All the paths defined remain exactly the same. Thus, when copying the deep storage segments to a new location, it is
required that the directory structure of the segments is unchanged. The only exception are directory names containing
colons (`:`) in cases when "hdfs" is the target storage - in this case every (`:`) has to be replaced by
underscore (`_`).

Usage: `update_loadSpec.py [-h] -i INPUT -o OUTPUT -t {hdfs,s3,google,azure} [-b BUCKET] [--delimiter DELIMITER]`
