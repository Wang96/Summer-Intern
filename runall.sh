# sh cross_valiFILE.sh -s 20702705 -l squared -d /grid/0/tmp/jaek/vw_train -r /homes/adwstg/jaek/pipeline_dev

# take input arguments
while getopts "f:l:d:r:" OPTION; do
    case $OPTION in
        f)  # file name
            FILE=$OPTARG     # ex) 20702705
            ;;
        l)  # type of loss function used for training
            LOSS_FUNCTION=$OPTARG   # ex) squared, hinge, logistic, quantile
            ;;
        d)  # directoy for vowpal wabbit input train data
            INPUT_PATH=$OPTARG  # ex) /grid/0/tmp/jaek/vw_train
            ;;
        r)  # root directory of the project (to find the library folders)
            PROJ_ROOT=$OPTARG   # ex) /homes/adwstg/jaek/pipeline_dev
            ;;
        \?)
            echo "You must feed FILE id (-s), type of loss function (-l) and path of the input train data (-d)"
            exit
            ;;
    esac
done

# shuffle the lines of the training data of a certain FILE
mkdir $INPUT_PATH/runall_$FILE
mkdir $INPUT_PATH/runall_$FILE/shuffled/
mkdir $INPUT_PATH/runall_$FILE/split/
mkdir $INPUT_PATH/runall_$FILE/train
mkdir $INPUT_PATH/runall_$FILE/test
mkdir $INPUT_PATH/runall_$FILE/tmp
mkdir $INPUT_PATH/result_$FILE


#
shuf $INPUT_PATH/$FILE -o $INPUT_PATH/runall_$FILE/shuffled/shuffled_$FILE

# get the number of lines N
NUM_LINE=$(wc -l < $INPUT_PATH/runall_$FILE/shuffled/shuffled_$FILE)
NUM_SPLIT=$(($NUM_LINE / 10))

if [ `expr $NUM_LINE % 10` -ne 0 ];
then NUM_SPLIT=$(($NUM_SPLIT+1));
fi

# split the training data into eqaul pieces of N/10 lines
split -l $NUM_SPLIT $INPUT_PATH/runall_$FILE/shuffled/shuffled_$FILE $INPUT_PATH/runall_$FILE/split/part_

# repeat follwing in 10 times : pick a piece, merge the rest, train on the merged one and test on the piece, save the accuracy, and remove the merged one
VW_ACCURACY=0.0

for part in $INPUT_PATH/runall_$FILE/split/*
do
    # move the testing set and create the training set by merging the rest
    mv $part $INPUT_PATH/runall_$FILE/test/
    cat $INPUT_PATH/runall_$FILE/split/* >> $INPUT_PATH/runall_$FILE/train/data_train

    # run vw and get the accuracy
    $PROJ_ROOT/vw $INPUT_PATH/runall_$FILE/train/data_train --passes 1 -l 0.1 --loss_function=$LOSS_FUNCTION -b 23 -f $INPUT_PATH/runall_$FILE/tmp/$FILE.model --cache_file $INPUT_PATH/runall_$FILE/tmp/$FILE.cache
    $PROJ_ROOT/vw -i $INPUT_PATH/runall_$FILE/tmp/$FILE.model -t $INPUT_PATH/runall_$FILE/test/* -p /dev/stdout --quiet > $INPUT_PATH/result_$FILE/test1
    $PROJ_ROOT/vw --audit -t -d $INPUT_PATH/runall_$FILE/test/* -i $INPUT_PATH/runall_$FILE/tmp/$FILE.model --quiet > $INPUT_PATH/result_$FILE/audit
    cat $INPUT_PATH/runall_$FILE/test/* | awk -F " | " '{print $1}' > $INPUT_PATH/result_$FILE/test0
    #$PROJ_ROOT/vw/bin64/./vw -b 30 -f $INPUT_PATH/runall_$FILE/tmp/$FILE.model -d $INPUT_PATH/runall_$FILE/train/data_train --loss_function=$LOSS_FUNCTION
    #$PROJ_ROOT/vw/bin64/./vw -d $INPUT_PATH/runall_$FILE/test/* -t -i $INPUT_PATH/runall_$FILE/tmp/$FILE.model -p $INPUT_PATH/runall_$FILE/tmp/pred_$FILE --binary
    VW_ACCURACY=$(echo "$VW_ACCURACY + $(python accuracy.py $INPUT_PATH/result_$FILE/test0 $INPUT_PATH/result_$FILE/test1)"|bc -l)

    # remove training set, prediction model, prediction result
    rm $INPUT_PATH/runall_$FILE/train/data_train
    rm $INPUT_PATH/runall_$FILE/tmp/$FILE.model
    rm $INPUT_PATH/runall_$FILE/tmp/$FILE.cache

    # put the testing set back
    mv $INPUT_PATH/runall_$FILE/test/* $INPUT_PATH/runall_$FILE/split/
done

python audit.py $INPUT_PATH/result_$FILE/audit #write to newaudit
awk -F ":" '{print $1,$4,$5}' newaudit | sort -n | uniq | sort -k3 -r -n | awk '{print $1,$2}' > $INPUT_PATH/result_$FILE/feature_weight

rm newaudit

# take the average of the accuracy and report it
echo $(echo "$VW_ACCURACY/10"|bc -l)

# clean the directories
rm -rf $INPUT_PATH/runall_$FILE