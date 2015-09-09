#!/bin/bash

DATE=`date +"%d%b%Y"`
TARBASEFILE=seqdepot.${DATE}.tar.gz
TARFILE=/flex/${TARBASEFILE}
DEVLOCATION=/dev/




T1=`date +"%s"`

echo "Creating the snapshot"
lvcreate -L5G --snapshot --name sdSnapShot /dev/vg/mongo_seqdepot || exit 1

echo "Mounting snapshot"
mkdir -p /media/storage7/backup/seqdepot
mount /dev/vg/sdSnapShot /media/storage7/backup/seqdepot || exit 1

echo "Creating tarball..."
cd /media/storage7/backup
tar -czf $TARFILE seqdepot
cd -

echo "Unmounting snapshot"
umount /dev/vg/sdSnapShot

echo "Removing snapshot device"
lvremove -f /dev/vg/sdSnapShot

chown ogun.users $TARFILE

#echo "Copying to alpha"
#su ulrich
#scp $TARFILE alpha:/mnt/backup

T2=`date +"%s"`
DELTA=$(($T2 - $T1))
echo "$(($DELTA / 60)) minutes and $(($DELTA % 60)) seconds"
