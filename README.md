# About

Tell us about your implementation and tests here

Handouts URL https://docs.google.com/document/d/18wI6L_46tsacsWvTpOBBoVAhrY-0Tzyctb3XIM6IbfY/edit?usp=sharing

How I'm running this:


docker run -it -v /Users/brian/Desktop/cs118/proj2_cloud:/raft-container/raft-main raft-container-go122

cd autograder-source/raft

cp -r /raft-container/raft-main/. submission/ && make test


cd submission
cp ../source/resources/studtest/test/ta_*.go test/

go test ./test -run 'TestMy311' -v -count=1 -timeout 300s 2>&1 | tee my311.log