In theory, you just need to execute `./migrate.sh`.

That shell script presupposes that:
- There is enough disk space for all the data + 2 copies of the data.
- The script can access the PostgreSQL database without specifying the credentials.
- The script can access the AWS S3 buckets without specifying the credentials.

If that is not the case, you will need to tweak the shell script. 
(In fact, you will probably tweak the shell script anyway since the names of databases and S3 buckets are hardcoded and they often change.)

After executing the shell script, you can use the SQL scripts inside the folder `athena` to create the tables on Athena.
