$server = "172.13.16.168"
$database = "inv"
$username = "emi"
$password = "Em1ss!"
$scriptFilePath = "N:/EMI_SERVER/INVENTAIRE/3_RESIDENTIEL/7_TEMPS_REEL/Scripts_sql/Res_calcul2.sql"

$command = "psql -h $server -U $username -d $database -f $scriptFilePath"

Invoke-Expression $command