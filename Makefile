# Makefile to install/update the iRODS SCC ruleset
#

install:
	cp scc-ruleset.re /etc/irods/

update:
	git pull
