
## file source -> persisted queue -> dev sink

###

```yaml
pipelines:
  - name: local
    sources:
      - type: file
        name: demo
        paths:
          - /tmp/log/*.log
        fields:
          topic: "loggie"
    
    queue:
      type: persisted
      path: /tmp
      maxFileBytes: 10MB
      batchBytes: 1MB
      
    sink:
      type: dev
      printMetrics: true
```

generate log files:

```
make genfiles LOG_TOTAL=300000
```



