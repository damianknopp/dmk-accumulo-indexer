#mvn assembly:single

if [ -z "$1" ]
then
	echo "Usage: `basename $0` [file-name]"
	exit $E_NOARGS
fi
java -cp ./target/accumulo-indexer-0.0.3-jar-with-dependencies.jar dmk.accumulo.Index dmk 127.0.0.1:2181 invertedIndex root pass 4 $1

return $?
