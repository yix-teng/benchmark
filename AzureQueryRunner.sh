#!/bin/sh

set -e

export AZURE_USER=sqladminuser
export AZURE_PWD='P@ssw0rd!'


#export AZURE_WAREHOUSE=sqlservlessinternalselfmanaged.sql.azuresynapse.net
#export AZURE_DATABASE=DeticatedSqlPool

#export AZURE_WAREHOUSE=sqlservlessinternalselfmanaged-ondemand.sql.azuresynapse.net
#export AZURE_DATABASE=sqlservlessinternalselfmanagedDB

AZURE_WAREHOUSE=sqlservlessinternalselfmanaged-ondemand.sql.azuresynapse.net
AZURE_DATABASE=sparkdb

tempdir=`mktemp -d _work_XXXXXXXXXX`

cleanup() {
  rm -rf "$tempdir" 2>/dev/null || :
}
trap cleanup TERM KILL

runsql() {
  args="-l 10 -N -m-1 -j -I -p"
  if [ "$1" = "bare" ]; then
    args="-N -I"
    shift 1
  fi
 echo sqlcmd $args \
    -S ${AZURE_WAREHOUSE} \
    -U "${AZURE_USER}" \
    -P "${AZURE_PWD}" \
    -d "${AZURE_DATABASE}" \
    $@
}

#sqlcmd -N -I -S sqlservlessinternalselfmanaged.sql.azuresynapse.net -U sqladminuser -P 'P@ssw0rd!' -d DeticatedSqlPool

upload() {
  args="-q -c -t| -e error.log"
  if ! bcp \
    $@ \
    $args \
    -S ${AZURE_WAREHOUSE} \
    -U "${AZURE_USER}" \
    -P "${AZURE_PWD}" \
    -d "${AZURE_DATABASE}"
  then
    echo "Failed to load with $@"
    cat error.log
    exit 1
  else
    echo "Success loading with $@"
  fi
}

timing() {
  mkfifo $tempdir/outsql

  touch $tempdir/output

  runsql -i "$1" >$tempdir/outsql &
  PID=$!

  cat $tempdir/outsql | {
    while read -r line ; do
      if echo "$line" | grep -q "Clock Time" ; then
        # Example: Clock Time (ms.): total        26  avg   26.0 (38.5 xacts per sec.)
        echo timing: $line 1>&7
        echo "$line" | awk '{print $5 / 1000}'
        break
      elif [ "$verbose" = 1 ]; then
        echo "sqlcmd: $line" 1>&7
      else
        echo $line >> $tempdir/output
      fi
    done
  }

  head -n15 $tempdir/output >&7

  wait $PID
  cleanup
}

if [ "$1" = "timing" ]; then
  time=`timing "$2" 7>&2`
  printf "%s,%.5f\n" "$2" "$time"
elif [ "$1" = "ddl" ]; then
  export verbose=1
  timing "$2" 7>&2
elif [ "$1" = "bcp" ]; then
  shift 1
  upload $@
else
  runsql bare
fi
