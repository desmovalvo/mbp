#!/bin/bash

# remove old ancillary files
if test -e X-po2s.db; then rm X-po2s.db ; fi
if test -e X-so2p.db; then rm X-so2p.db ; fi
if test -e X-sp2o.db; then rm X-sp2o.db ; fi

# start the ancillary sib
redsibd &
sib-tcp --port 10088