cd ../mrapps/
go build -buildmode=plugin wc.go
cd ../main/

if [ -e "mr-out-tmp" ]; then 
    rm mr-out-tmp
fi

for value in {1..20}  
do  
     go run mrworker.go ../mrapps/wc.so
done 