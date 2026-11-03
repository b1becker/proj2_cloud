zipball:
	rm -f submission.zip
	zip -r submission.zip . -x ".git/*" "submission.zip" "vendor/*"