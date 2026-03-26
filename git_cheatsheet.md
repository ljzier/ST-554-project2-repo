#GIT Cheatsheet

##commit workflow

cd ~/ST-554-project2-repo

git add .
git status
git commit -m "accomplishments"
git push

####### First time only
git cogit config --global user.email "ljzier@ncsu.edu"
git config --global user.name "Linda"
git clone https://github.com/YOUR_USERNAME/ST-554-project2-repo.git

######## if you just want current copy (no merge)
git push --force