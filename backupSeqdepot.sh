#!/bin/bash

DATE=`date +"%d%b%Y"`
TARBASEFILE=seqdepot.${DATE}.tar.gz
TARFILE=/flex/${TARBASEFILE}

T1=`date +"%s"`

echo "Creating the snapshot"
lvcreate -L5G --snapshot --name sdSnapShot /dev/vg/mongo_seqdepot || exit 1

echo "Mounting snapshot"
mkdir -p /mongo/backup/seqdepot
mount /dev/vg/sdSnapShot /mongo/backup/seqdepot || exit 1

echo "Creating tarball..."
cd /mongo/backup
tar -czf $TARFILE seqdepot
cd -

echo "Unmounting snapshot"
umount /dev/vg/sdSnapShot

echo "Removing snapshot device"
lvremove -f /dev/vg/sdSnapShot

chown ulrich.users $TARFILE

echo "Copying to alpha"
su ulrich
scp $TARFILE alpha:/mnt/backup

T2=`date +"%s"`
DELTA=$(($T2 - $T1))
echo "$(($DELTA / 60)) minutes and $(($DELTA % 60)) seconds"
