#for d in Chapter3 Chapter4 Chapter5 Chapter6 Chapter7 Chapter8 
for d in Chapter5
do
  echo $d
  cd $d
  echo "bq mk $d"
  bq mk $d
  /usr/local/Cellar/python/3.6.5_1/bin/python3 ../sql2bq.py > insert.sh
  sh insert.sh
  cd ..
  echo ''
done
