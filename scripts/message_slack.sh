#message handed to this script needs to be contained in quotes

#message is sent to Baderlab slack genemania_build_status channel 
#(that is what the webhooks specifies)
#For info on setting slack up for this see:https://api.slack.com/tutorials/slack-apps-hello-world

curl -X POST -H 'Content-type: plication/json' --data '{"text":"'"$@"'"}' https://hooks.slack.com/services/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

