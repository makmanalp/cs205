sudo python -mpip install -U pip
sudo python -mpip install pandas networkx matplotlib

mkdir -p /home/hadoop/data/
wget -O /home/hadoop/data/facebook_combined.gz https://snap.stanford.edu/data/facebook_combined.txt.gz
wget -O /home/hadoop/data/gplus_combined.gz https://snap.stanford.edu/data/gplus_combined.txt.gz
wget -O /home/hadoop/data/asoiaf-all-edges.csv https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-all-edges.csv

sudo yum install -y git
git clone https://github.com/makmanalp/cs205 /home/hadoop/cs205/

echo -e '\nexport PYTHONPATH=/home/hadoop/cs205/:$PYTHONPATH\n' >> ~/.bashrc
#export PYTHONPATH=/home/hadoop/cs205/:$PYTHONPATH

#spark-submit --master 'local[*]'  --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11 /home/hadoop/cs205/src/experiments/run.py
