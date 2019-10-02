#!/bin/bash
bundle install
ruby app.rb $@
ruby irg-instructie-to-combo.rb
