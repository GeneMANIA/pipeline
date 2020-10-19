#message handed to this script needs to be contained in quotes

#message is sent to Baderlab slack genemania_build_status channel 
#(that is what the webhooks specifies)
#For info on setting slack up for this see:https://api.slack.com/tutorials/slack-apps-hello-world

curl -X POST -H 'Content-type: application/json' --data '{"text":"'"$@"'"}' `cat /home/gmbuild/sm_build_org/slack_webhook`

