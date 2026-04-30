#!/bin/bash
# Beets twice-daily: import new music, fetch art, embed art
/usr/bin/docker exec beets beet import /music >> /DATA/AppData/beets/cron.log 2>&1
/usr/bin/docker exec beets beet fetchart >> /DATA/AppData/beets/cron.log 2>&1
/usr/bin/docker exec beets beet embedart >> /DATA/AppData/beets/cron.log 2>&1
