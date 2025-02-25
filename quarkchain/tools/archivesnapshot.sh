sshpass -p $PriKey scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null u446960@u446960.your-storagebox.de:public-data/VERSION VERSION

filename=`cat VERSION`

if [ $filename = "Archive-A.tar.gz" ]; then
  filename="Archive-B.tar.gz"
else
  filename="Archive-A.tar.gz"
fi
echo $filename


rm $filename
curl https://s3-us-west-2.amazonaws.com/qkcmainnet/data/`curl https://s3-us-west-2.amazonaws.com/qkcmainnet/data/LATEST`.tar.gz --output $filename

sshpass -p $PriKey scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $filename u446960@u446960.your-storagebox.de:public-data/$filename

echo $filename > VERSION

sshpass -p $PriKey scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null VERSION u446960@u446960.your-storagebox.de:public-data/VERSION