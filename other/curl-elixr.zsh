#!/bin/zsh

curl 'https://chosen-hydra.vidangel.com/graphql' \
  -H 'content-type: application/json' \
  --data-raw '{"query":"query allOfTheSeasonsANdTheirFunding {\n  seasonFunding {\n\t\tbackers\n    currency\n    days\n    episodeBackers\n    episodeCost\n    episodeCurrentlyFunding\n    episodeFundingIn\n    episodeFundingTimeUnit\n    episodePercentComplete\n    episodeRaised\n    progress\n    seasonCurrentlyFunding\n    timeLeft\n    timeUnit\n    totalAmountRaised\n\t}\n}","variables":null,"operationName":"allOfTheSeasonsANdTheirFunding"}' | jq