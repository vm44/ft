test

Remarks:

For simplicity, all classes in one file.

Log record use whitespaces as delimiters.

Gradle build & config files created by IntelliJ IDEA.

About grouping: It seems to me, that is better make grouping by _field_ user instead of _value_ user.
Program currently ignoring user name in grouping operation.

Btw, excellent library jCommander, used for cmd line parsing, currently works only with one "command"
(see jCommander inner terms) in cmd line (has opened issue on GitHub)

cmd line example
program -t 2 -o out.log -u s -a m filter -u Pete -p patt -f 2015-04-05T10:10:10 -t 2017-10-10T10:10:10
