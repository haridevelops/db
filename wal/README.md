Step 1: Install protoc
```
curl -kLO https://github.com/protocolbuffers/protobuf/releases/download/v26.0/protoc-26.0-osx-x86_64.zip
unzip protoc-26.0-osx-x86_64.zip -d protoc
sudo mv protoc/bin/protoc /usr/local/bin/
chmod +x $(which protoc)
```


Step 2: Install protoc-gen-go
```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
ls "$(go env GOPATH)/bin/protoc-gen-go"
echo 'export PATH="$PATH:$(go env GOPATH)/bin"' >> ~/.zshrc
source ~/.zshrc
which protoc-gen-go
protoc --go_out=. --go_opt=paths=source_relative wal_data_log.proto
```
