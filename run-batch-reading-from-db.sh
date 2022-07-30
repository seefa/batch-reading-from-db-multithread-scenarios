# shellcheck disable=SC2006
CURRENT_DATE=`date '+%Y%m%d-%H%M%S'`
CURRENT_PATH=$(basename "$PWD")
CURRENT_USER=$USER

echo "Batch processing started on $CURRENT_DATE by $CURRENT_USER in $CURRENT_PATH"

./gradlew clean build

java -jar ./build/libs/batch-reading-from-db-in-multithread-scenarios-*.*.*-SNAPSHOT.jar "CURRENT_DATE=$CURRENT_DATE"

# shellcheck disable=SC2162
read;