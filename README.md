# 아파치 스톰 기반 악성 URL / DGA 탐지 프로그램

악성 URL / DGA 탐지 모델을 아파치 스톰의 구조에 맞게 분산 실행한 프로그램 

## 분산 병렬 처리 알고리즘

![structure](./image/structure.png)

- Kafka Spout
- CNN Bolt
- LSTM Bolt
- GRU Bolt
- Final Bolt
- Kafka Bolt

## 실행 방법

Maven을 install 한 후 jar 파일을 서버에서 실행

```
$ storm jar StormStacking-1.0-SNAPSHOT.jar storm.StackingTopology <INPUT_TOPIC> <OUTPUT_TOPIC> <EX_TIME> <ZK_SERVER> <KAFKA_SERVER> <TOPOLOGY_NAME>
```

- `<INPUT_TOPIC>`: 카프카에서 데이터를 입력 받을 토픽
- `<OUTPUT_TOPIC>`: 카프카로 데이터를 출력할 토픽
- `<EX_TIME>`: 토폴로지 실행 시간 (default: 300)
- `<ZK_SERVER>`: 카프카가 실행된 주키퍼 서버 정보 (default: MN:2181,SN01:2181,SN02:2181,SN03:2181) 
- `<KAFKA_SERVER>`: 카프카 서버 정보 (default: MN:9092,SN01:9092,SN02:9092,SN03:9092)
- `<TOPOLOGY_NAME>`: 실행될 토폴로지의 이름 (default: StormStacking)

## 데이터

### 입력데이터
URL 데이터
```
https://github.com/hjmoons/storm-inference
```

### 출력데이터
URL 데이터 + 데이터 분류 결과
```$xslt
https://github.com/hjmoons/storm-inference,0.000878
```

