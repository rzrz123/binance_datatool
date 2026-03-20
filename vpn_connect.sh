#!/usr/bin/expect -f

# Connect to Surfshark VPN - Japan Tokyo / UDP
spawn sudo surfshark-vpn

# Keep pressing enter through any number of "press enter for next page" prompts
while 1 {
    expect {
        "press enter for next page" { send "\r" }
        "Enter a number to select the location" { break }
    }
}

send "58\r"
expect "Enter a number to select the VPN connection type"
send "1\r"
expect eof
