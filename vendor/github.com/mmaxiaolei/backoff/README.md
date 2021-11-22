# backoff
go backoff lib,support jitter,avoid 'thundering herd' effect

## install

`go get github.com/mmaxiaolei/backoff`

## usage

```go
b := backoff.NewExponentialBackoff()
d := b.Next()
```
