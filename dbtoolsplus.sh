#! /bin/bash

function info() {
	echo -e "\tbash $0"
}
#function info() {
#	echo -e "\tPlease swith to your own database connection address infomation."
#	echo -e "\tSet number of concurrent."
#	echo -e "\tUse:"
#	echo -e "\t\tbash $0 <options>"
#	echo -e "\tOption:"
#	echo -e "\t\tdumpddl\t\tDownload the table structure of public schema under the entire database."
#	echo -e "\t\t\t\tuse: bash $0 <database name> dumpddl [nobak]\n"
#	echo -e "\t\tcreate_table\tCreate table."
#	echo -e "\t\t\t\tuse: bash $0 <database name> create_table <table structure file> [noindex]\n"
#	echo -e "\t\tcreate_index\tCreate index."
#	echo -e "\t\t\t\tuse: bash $0 <database name> create_index <table structure file>\n"
#	echo -e "\t\tdrop_table\tDrop the database all table."
#	echo -e "\t\t\t\tuse: bash $0 <database name> drop_table\n"
#	echo -e "\t\ttruncate_table\tTruncate the database all table."
#	echo -e "\t\t\t\tuse: bash $0 <database name> truncate_table\n"
#	echo -e "\t\tdrop_index\tdrop the database all table's indexes."
#	echo -e "\t\t\t\tuse: bash $0 <database name> drop_index\n"
#	echo -e "\t\t-h, --help\tView help."
#	echo -e "\n"
#}

function Colour (){
	RED="\033[31m"
	GREEN="\033[32m"
	RESET="\033[0m"
}

function GetEnv (){
	LOOPCODE=false
	while ! ${LOOPCODE}
	do
		read -p "Please input host (default:127.0.0.1): " -e HOST
		if [ -z ${HOST} ]; then HOST="127.0.0.1"; fi
		read -p "Please input database (default:postgres): " -e DATABASE
		if [ -z ${DATABASE} ]; then DATABASE="postgres"; fi
		read -p "Please input database prot (default:20158): " -e DBPORT
		if [ -z ${DBPORT} ]; then DBPORT="20158"; fi
		read -p "Please input database user (default:postgers): " -e DBUSER
		if [ -z ${DBUSER} ]; then DBUSER="postgers"; fi
		read -p "Please input user password: " -s DBPASSWD
		if [ -z ${DBPASSWD} ]; then DBPASSWD="postgres"; fi
		echo ""
		CONNECTION="postgres://${DBUSER}:${DBPASSWD}@${HOST}:${DBPORT}/${DATABASE}?sslmode=require"
		CONNECTIONR="postgres://${DBUSER}@${HOST}:${DBPORT}/${DATABASE}"
		read -p "postgres connection url \"${CONNECTIONR}\" [Yes/No]: " -e LOOPEND
		if [ -z ${LOOPEND} ] || [ ${LOOPEND} = "Y" ] || [ ${LOOPEND} = "Yes" ] || [ ${LOOPEND} = "YES" ] || [ ${LOOPEND} = "yes" ] || [ ${LOOPEND} = "y" ];then LOOPCODE=true; fi
	done
	echo -e "HOST=${HOST}\nDATABASE=${DATABASE}\nDBPORT=${DBPORT}\nDBUSER=${DBUSER}" > ./.latest_postgres_conection
}

function RunConnection() {
	if [ -e ./.latest_postgres_conection ];then
		source ./.latest_postgres_conection
		CONNECTIONR="postgres://${DBUSER}@${HOST}:${DBPORT}/${DATABASE}"
		read -p "postgres connection url \"${CONNECTIONR}\" [Yes/No]: " -e CONNCODE
		if [ -z ${CONNCODE} ] || [ ${CONNCODE} = "Y" ] || [ ${CONNCODE} = "Yes" ] || [ ${CONNCODE} = "YES" ] || [ ${CONNCODE} = "yes" ] || [ ${CONNCODE} = "y" ];then
			read -p "Please input user password: " -s DBPASSWD
			if [ -z ${DBPASSWD} ]; then DBPASSWD="postgres"; fi
			CONNECTION="postgres://${DBUSER}:${DBPASSWD}@${HOST}:${DBPORT}/${DATABASE}?sslmode=require"
		else
			GetEnv
		fi
	else
		GetEnv
	fi
}

function Connection() {
	${EXECC} ${CONNECTION} -e "select 1 from dual;" > .tmp_dbtools_connect_into 2>&1
	if [ $? -ne 0 ]; then
		read -p "Please input exec file: " -s EXECFILE
		EXECC="${EXECFILE} --connect"
		${EXECC} ${CONNECTION} -e "select 1 from dual;" > .tmp_dbtools_connect_into 2>&1
	fi

	CONN=$(sed -n 2p .tmp_dbtools_connect_into)
	if [ ! -z ${CONN} ] && [ ${CONN} == "1" ];then
		echo -e "database connection ${GREEN}succeeded${RESET}."
		rm -f .tmp_dbtools_connect_into
	else
		echo -e "database connection ${RED}failed${RESET}."
		cat .tmp_dbtools_connect_into
		rm -f .tmp_dbtools_connect_into
		exit 0
	fi
}

function FuncInfo() {
	SEG="+#+#+\n"
	TITLE="| No#| function#| describe#|\n"
	DUMPDDL="| 1#| dumpddl#| Download all table structures of the target database.#|\n"
	CREATETAB="| 2#| create_table#| Create all table.#|\n"
	CREATEIDX="| 3#| create_index#| Create all index.#|\n"
	DROPTAB="| 4#| drop_table#| Drop the database all table.#|\n"
	DROPIDX="| 5#| drop_index#| drop the database all table's indexes.#|\n"
	TRUNCATETAB="| 6#| truncate_table#| Truncate the database all table.#|\n"
	STATISTICS="| 7#| create_statistics#| create database all table statistics.#|\n"
	COMMENTS="| 8#| create_comments#| create database all table commnets.#|\n"
	TEXT="${DUMPDDL}${CREATETAB}${CREATEIDX}${DROPTAB}${DROPIDX}${TRUNCATETAB}${STATISTICS}${COMMENTS}"
	TABLE=${SEG}${TITLE}${SEG}${TEXT}${SEG}
	echo -e ${TABLE} | column -s "#" -t | awk '{if($0 ~ /^+/){gsub(" ","-",$0);print $0}else{print $0}}'
}

function FuncPre() {
	STARTED=$(date +%y%m%d%H%M%S)
	LOGPATH=db_tools_logs
	FUNLOG=${LOGPATH}/${1}_${STARTED}
	if [[ ! -d ${FUNLOG} ]];then
		mkdir -p ${FUNLOG}
	fi
}

function currentNum() {
	read -p "Please enter the concurrent number (default: $1): " -e CONCURRENCY_NUMBER
	if [ -z ${CONCURRENCY_NUMBER} ]; then CONCURRENCY_NUMBER="$1"; fi
}

function dumpddl() {
	FuncPre dumpddl
	DUMPFILE=${FUNLOG}/${DATABASE}.sql
	${EXECC} ${CONNECTION} -e "select create_statement from dbms_internal.create_statements where schema_name = 'public';" > ${DUMPFILE} 2>&1
	sed -i '1d' ${DUMPFILE}
	sed -i 's/"CREATE/CREATE/g' ${DUMPFILE}
	sed -i 's/)"/);/g' ${DUMPFILE}
	sed -i 's/""/"/g' ${DUMPFILE}
	sed -i '{/COMMENT ON /s/\"$/;/g}' ${DUMPFILE}
	sed -i '/CREATE VIEW/ s/$/&;/' ${DUMPFILE}
	echo -e "dump file: ${GREEN}${PWD}/${DUMPFILE}${RESET}"
}

function inputFile() {
	read -p "Please enter the script file (default:${DATABASE}.sql): " -e SCRIPTFILE
	if [ -z ${SCRIPTFILE} ]; then SCRIPTFILE="${DATABASE}.sql"; fi
	if [ ! -f ${SCRIPTFILE} ]; then echo "${DATABASE}.sql not exist, please re-enter!"; getFile; fi
}


function getFile() {
	inputFile
	read -p "Whether file windows to unix conversion is required? [Yes/No] (default: No): " -e CONV
	#if [ -z ${CONV} ]; then CONV="no"; fi
	
	if [ -z ${CONV} ] || [ ${CONV} = "N" ] || [ ${CONV} = "No" ] || [ ${CONV} = "no" ] || [ ${CONV} = "NO" ]; then 
		CONV="no"
	elif [ ${CONV} = "Y" ] || [ ${CONV} = "Yes" ] || [ ${CONV} = "YES" ] || [ ${CONV} = "yes" ] || [ ${CONV} = "y" ]; then
		CONV="yes"
	else
		echo "Input error!"
	fi

	if [ ${CONV} == "yes" ]; then 
		dos2unix ${SCRIPTFILE}
		if [ $? -ne "0" ]; then
			echo -e "Please install dos2unix!\nfor example:\nsudo yum install dos2unix"
			exit 0
		fi
	fi
}


function CreateComm() {
	getFile
	currentNum 50
	FuncPre create_comments
	LOGFILE=${FUNLOG}/create_comments.log
	echo "log file: ${PWD}/${LOGFILE}"

	COMMFILE=${FUNLOG}/comment_file.sql
	grep "COMMENT ON " ${SCRIPTFILE} >> ${COMMFILE}
	sed -i '1i\\\set errexit false;' ${COMMFILE}
	${EXECC} ${CONNECTION} -f ${COMMFILE} >> ${LOGFILE} 2>&1
	echo "$(date "+%Y-%m-%d %T") Create commnets done!" >> ${LOGFILE} 2>&1

	# FILEROW=$(wc -l ${COMMFILE} | awk '{print $1}')
	# FILENUM=${CONCURRENCY_NUMBER}
	# FILENUMROW=$(( ${FILEROW} + ${FILENUM} - 1 ))
	# EFILENUMROW=$(( ${FILENUMROW}/${FILENUM} ))
	# split -d -a 5 -l ${EFILENUMROW} ${COMMFILE} --additional-suffix=.sql comments_split_

}


function currentCreateTab() {
	DBFILE=${1}
	SETABCOMM=${2}
	COMMENTFILE=${FUNLOG}/comment_file.sql
	TABLESFILE=${FUNLOG}/tables_file.sql
	COMMENTPATH=${FUNLOG}/comment
	TABLEPATH=${FUNLOG}/table

	touch ${COMMENTFILE} ${TABLESFILE}
	mkdir -p ${COMMENTPATH} ${TABLEPATH}

	grep "COMMENT ON " ${DBFILE} >> ${COMMENTFILE}
	#split -d -l 200 ${COMMENTFILE} --additional-suffix=.sql ${COMMENTPATH}/comments_split_
	#COMMFILE=$(ls ${COMMENTPATH})

	cp ${DBFILE} ${TABLESFILE}
	sed -i '/COMMENT ON /d' ${TABLESFILE}

	

	TABLENAME=$(grep "CREATE TABLE" ${TABLESFILE} | awk '{print $3}')

	[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
	exec 5<>/tmp/fd1
	rm -rf /tmp/fd1
	for (( i=1;i<=${CONCURRENCY_NUMBER};i++ ))
	do
		echo >&5
	done

	for i in ${TABLENAME}
	do
		read -u5
	{
		TABLEFILE=${TABLEPATH}/${i}.sql
		awk '/CREATE TABLE '${i}' /,/;/{print $0}' ${TABLESFILE} > ${TABLEFILE} 2>&1
		HASHCODE=$(grep -i "USING HASH" ${TABLEFILE} | wc -l)
		
		if [[ ${HASHCODE} -eq 0 ]];then
			${EXECC} ${CONNECTION} -f ${TABLEFILE} >> ${LOGFILE} 2>&1
		else
			sed -i '1i\set experimental_enable_hash_sharded_indexes=true;' ${TABLEFILE}
			${EXECC} ${CONNECTION} -f ${TABLEFILE} >> ${LOGFILE} 2>&1
		fi
		echo >&5
	}&	
	done
	
	wait
	exec 5<&-
	exec 5>&-
	echo "$(date "+%Y-%m-%d %T") create table done!" >> ${LOGFILE} 2>&1

	if [[ ${SETABCOMM} == "yes" ]];then
		${EXECC} ${CONNECTION} -f ${COMMENTFILE} >> ${LOGFILE} 2>&1
		echo "$(date "+%Y-%m-%d %T") Create commnets done!" >> ${LOGFILE} 2>&1
	else
		echo "$(date "+%Y-%m-%d %T") Not create commnets!" >> ${LOGFILE} 2>&1
	fi
}


function createTab() {
	getFile
	read -p "Whether to create indexes when creating tables? [Yes/No] (default:Yes): " -e TABIDX
	if [ -z ${TABIDX} ] || [ ${TABIDX} = "Y" ] || [ ${TABIDX} = "Yes" ] || [ ${TABIDX} = "YES" ] || [ ${TABIDX} = "yes" ] || [ ${TABIDX} = "y" ]; then 
		TABIDX="yes"
	elif [ ${TABIDX} = "N" ] || [ ${TABIDX} = "No" ] || [ ${TABIDX} = "no" ] || [ ${TABIDX} = "NO" ]; then
		TABIDX="no"
	else
		echo "Input error!"
	fi

	read -p "Whether to create comments when creating tables? [Yes/No] (default:Yes): " -e TABCOMM
	if [ -z ${TABCOMM} ] || [ ${TABCOMM} = "Y" ] || [ ${TABCOMM} = "Yes" ] || [ ${TABCOMM} = "YES" ] || [ ${TABCOMM} = "yes" ] || [ ${TABCOMM} = "y" ]; then 
		TABCOMM="yes"
	elif [ ${TABCOMM} = "N" ] || [ ${TABCOMM} = "No" ] || [ ${TABCOMM} = "no" ] || [ ${TABCOMM} = "NO" ]; then
		TABCOMM="no"
	else
		echo "Input error!"
	fi

	currentNum 200
	FuncPre create_table
	LOGFILE=${FUNLOG}/createTab.log
	echo "log file: ${PWD}/${LOGFILE}"
	NIDXFILE=${FUNLOG}/noindx_${SCRIPTFILE}
	if [[ ${TABIDX} == "no" ]];then
		cp ${SCRIPTFILE} ${NIDXFILE}
		sed -i '/INDEX /d' ${NIDXFILE}
		#sed -i '1i\set experimental_enable_hash_sharded_indexes=true;' ${NIDXFILE}
		#sed -i '1i\\\set errexit false;' ${NIDXFILE}
		#${EXECC} ${CONNECTION} -f ${NIDXFILE} >> ${LOGFILE} 2>&1
		currentCreateTab ${NIDXFILE} ${TABCOMM}
	else
		currentCreateTab ${SCRIPTFILE} ${TABCOMM}
		#sed -i '1i\set experimental_enable_hash_sharded_indexes=true;' ${SCRIPTFILE}
		#sed -i '1i\\\set errexit false;' ${SCRIPTFILE}
		#${EXECC} ${CONNECTION} -f ${SCRIPTFILE} >> ${LOGFILE} 2>&1
	fi
	

	#CREATE_NUM=$(${EXECC} ${CONNECTION} -e "select count(name) from dbms_internal.tables where database_name = '${DATABASE}';" | awk '{print ${2}}')
	CREATE_NUM=$(egrep "^CREATE TABLE" ${LOGFILE} | wc -l)
	SCRIPT_NUM=$(egrep -i "CREATE.*.TABLE" ${SCRIPTFILE} | wc -l)
	if [ ${SCRIPT_NUM} -eq ${CREATE_NUM} ];then
		echo -e "Script table numbers: ${SCRIPT_NUM}, database table numbers: ${GREEN}${CREATE_NUM}${RESET}"
		echo -e "Create table ${GREEN}succeeded${RESET}!"
	else
		echo -e "Script table numbers: ${SCRIPT_NUM}, database table numbers: ${RED}${CREATE_NUM}${RESET}"
		echo -e "Create table ${RED}failed${RESET}!"
	fi

}



function createIdx() {
	getFile
	currentNum 50
	FuncPre create_index

	CIDXPATH=${FUNLOG}
	mkdir ${CIDXPATH}/.tmp ${CIDXPATH}/.idx
	LOGFILE=${CIDXPATH}/create_index.log
	TABLENAME=$(grep "CREATE TABLE" ${SCRIPTFILE} | awk '{print $3}')
	echo "log file: ${PWD}/${LOGFILE}"

	[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
	exec 5<>/tmp/fd1
	rm -rf /tmp/fd1
	for (( i=1;i<=${CONCURRENCY_NUMBER};i++ ))
	do
		echo >&5
	done

	for i in ${TABLENAME}
	do
		read -u5
	{
		INXFILE=${CIDXPATH}/.idx/${i}.sql
		TABLEFILE=${CIDXPATH}/.tmp/${i}.sql
		awk '/CREATE TABLE '${i}' /,/;/{print $0}' ${SCRIPTFILE} > ${TABLEFILE} 2>&1
		grep "INDEX " ${TABLEFILE} | sed 's/^M//g' | sed '{s/,$/;/g}' | sed 's/^[ \t]*//g' > ${INXFILE} 2>&1
		
		if [[ -s ${INXFILE} ]];then
			awk -F "(" '{print "CREATE "$1"ON '$i' ("$2 > "'${INXFILE}'"}' ${INXFILE} 
			sed -i '1i\\\set errexit false;' ${INXFILE}
			${EXECC} ${CONNECTION} -f ${INXFILE} >> ${LOGFILE} 2>&1 
		else
			rm -rf ${INXFILE}
		fi
		echo >&5
	}&
		
	done
	wait
	exec 5<&-
	exec 5>&-
}

function dropTab() {
	currentNum 50
	FuncPre drop_table
	DTABPATH=${FUNLOG}
	LOGFILE=${DTABPATH}/drop_table.log
	TABNAMEFILE=${DTABPATH}/table_name.txt
	${EXECC} ${CONNECTION} -e "select table_name from information_schema.tables where table_schema = 'public';" > ${TABNAMEFILE} 2>&1
	sed -i '1d' ${TABNAMEFILE}
	echo "log file: ${PWD}/${LOGFILE}"
	[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
	exec 5<>/tmp/fd1
	rm -rf /tmp/fd1
	for (( i=1;i<=${CONCURRENCY_NUMBER};i++ ))
	do
		echo >&5
	done

	for i in $(cat ${TABNAMEFILE})
	do
		read -u5
	{
		# echo $i
		${EXECC} ${CONNECTION} -e "drop table $i cascade;" >> ${LOGFILE} 2>&1
		echo >&5
	}&

	done
	wait
	exec 5<&-
	exec 5>&-
}

function dropIdx() {
	currentNum 50
	FuncPre drop_index
	DIDXPATH=${FUNLOG}
	LOGFILE=${DIDXPATH}/drop_index.log
	IDXNAMEFILE=${DIDXPATH}/index.sql
	DROPFILE=${DIDXPATH}/drop_index.sql
	${EXECC} ${CONNECTION} -e "select indexdef from pg_catalog.pg_indexes where schemaname = 'public' and indexname != 'primary';" >> ${IDXNAMEFILE} 2>&1
	sed -i '1d' ${IDXNAMEFILE}
	sed -i 's/$/;/g' ${IDXNAMEFILE}
	awk  '{if ($2 == "INDEX") {print "DROP INDEX "$5"@"$3";"} else {print "DROP INDEX "$6"@"$4" CASCADE;"}}' ${IDXNAMEFILE} >> ${DROPFILE} 2>&1
	echo "log file: ${PWD}/${LOGFILE}"
	[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
	exec 5<>/tmp/fd1
	rm -rf /tmp/fd1
	for (( i=1;i<=${CONCURRENCY_NUMBER};i++ ))
	do
		echo >&5
	done

	while read -r i
	do
		read -u5
	{
		# echo $i
		${EXECC} ${CONNECTION} -e "$i" >> ${LOGFILE} 2>&1
		echo >&5
	}&

	done < ${DROPFILE}

	wait
	exec 5<&-
	exec 5>&-
}

function truncateTab() {
	currentNum 50
	FuncPre truncate_table
	TRTABPATH=${FUNLOG}
	LOGFILE=${TRTABPATH}/truncate_table.log
	TABNAMEFILE=${TRTABPATH}/table_name.txt
	${EXECC} ${CONNECTION} -e "select table_name from information_schema.tables where table_schema = 'public';" > ${TABNAMEFILE} 2>&1
	echo "log file: ${PWD}/${LOGFILE}"
	sed -i '1d' ${TABNAMEFILE}
	[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
	exec 5<>/tmp/fd1
	rm -rf /tmp/fd1
	for (( i=1;i<=${CONCURRENCY_NUMBER};i++ ))
	do
		echo >&5
	done
	for i in $(cat ${TABNAMEFILE})
	do
		read -u5
	{
		${EXECC} ${CONNECTION} -e "truncate table $i;" >> ${LOGFILE} 2>&1
		echo >&5
	}&
	done
	wait
	exec 5<&-
	exec 5>&-
}

function CreateStat() {
	currentNum 1000
	FuncPre create_statistics
	LOGFILE=${FUNLOG}/create_statistics.log
	TABNAMEFILE=${FUNLOG}/table_name.txt
	${EXECC} ${CONNECTION} -e "select table_name from information_schema.tables where table_schema = 'public';" > ${TABNAMEFILE} 2>&1
	echo "log file: ${LOGFILE}"
	sed -i '1d' ${TABNAMEFILE}

	[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
	exec 5<>/tmp/fd1
	rm -rf /tmp/fd1
	for (( i=1;i<=${CONCURRENCY_NUMBER};i++ ))
	do
		echo >&5
	done
	for i in $(cat ${TABNAMEFILE})
	do
		read -u5
	{
		${EXECC} ${CONNECTION} -e "CREATE STATISTICS ${i}_${STARTED} from ${i} as of system time '-10s'" >> ${LOGFILE} 2>&1
		echo >&5
	}&
	done
	wait
	exec 5<&-
	exec 5>&-
}



function SelectFunc() {
	echo "The following is a list of funxtions:"
	FuncInfo
	read -p "Please select function: " -e FUNCSELECT
	case ${FUNCSELECT} in 
		"dumpddl" | 1)
			dumpddl
			;;
		"create_table" | 2)
			createTab
			;;
		"create_index" | 3)
			createIdx
			;;
		"drop_table" | 4)
			dropTab
			;;
		"drop_index" | 5)
			dropIdx
			;;
		"truncate_table" | 6)
			truncateTab
			;;
		"create_statistics" | 7)
			CreateStat
			;;
		"create_comments" | 8)
			CreateComm
			;;
		*)
			echo -e "${RED}Input error, program exit!${RESET}"
			exit 0
			;;
	esac



}

function main() {
	EXECC="cockrach --url"
	Colour
	RunConnection
	Connection
	SelectFunc

}

# runing
if [[ ${1} = "--help" ]] || [[ ${1} = "-h" ]]; then
	info
	exit 0
fi

Colour
main

