# spring-batch-s3-extension

> A Spring Batch extension library for seamless integration with Amazon S3.

This library provides ready-to-use Spring Batch components to interact with AWS S3
via [Spring Cloud AWS](https://awspring.io/). Each component (writer, reader, processor, ...) is designed to plug
directly into your existing Spring Batch jobs without boilerplate.

---

## 📦 Requirements

| Dependency       | Version |
|------------------|---------|
| Java             | 21+     |
| Spring Boot      | 3.5+    |
| Spring Batch     | 5.x     |
| Spring Cloud AWS | 3.4+    |
| Maven            | 3.x     |

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/benjaminparsy/spring-batch-s3-extension.git
cd spring-batch-s3-extension
```

### 2. Build

```bash
./mvnw clean install
```

### 3. Add the dependency

```xml

<dependency>
    <groupId>com.benjamin.parsy</groupId>
    <artifactId>spring-batch-s3-extension</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## 🧩 Components

### ✍️ S3ItemWriter

Writes batch output directly to an S3 bucket. It handles buffering, automatic multipart upload, and full restartability
out of the box.

**Key features:**

- Buffers data in memory and automatically switches to S3 multipart upload when the buffer threshold is reached (
  default: **5 MB**)
- Falls back to a simple `PutObject` call for small files that never exceed the threshold
- Optional `S3HeaderCallback` / `S3FooterCallback` to prepend/append content to the file
- Fully restartable via Spring Batch's `ExecutionContext`
- Automatically aborts multipart uploads on failure

**Usage:**

```java

@Bean
public S3ItemWriter<MyItem> s3ItemWriter(S3Client s3Client) {
    return new S3ItemWriterBuilder<MyItem>()
            .s3Client(s3Client)
            .location(Location.of("s3://my-bucket/output/my-file.csv"))
            .byteConverter(chunk -> {
                StringBuilder sb = new StringBuilder();
                for (MyItem item : chunk) {
                    sb.append(item.toCsvLine()).append("\n");
                }
                return sb.toString().getBytes(StandardCharsets.UTF_8);
            })
            .headerCallback(() -> "id,name,value\n".getBytes(StandardCharsets.UTF_8))
            .footerCallback(() -> "## END OF FILE\n".getBytes(StandardCharsets.UTF_8))
            .bufferSize(DataSize.ofMegabytes(10)) // optional, default is 5 MB
            .build();
}
```

**Wiring into a step:**

```java

@Bean
public Step myStep(JobRepository jobRepository,
                   PlatformTransactionManager transactionManager,
                   ItemReader<MyItem> reader,
                   S3ItemWriter<MyItem> writer) {
    return new StepBuilder("myStep", jobRepository)
            .<MyItem, MyItem>chunk(100, transactionManager)
            .reader(reader)
            .writer(writer)
            .build();
}
```

---

> 🚧 More components are on their way — see the [Roadmap](#️-roadmap) below.

---

## 🧪 Testing

Tests use [Testcontainers](https://testcontainers.com/) with [Adobe S3Mock](https://github.com/adobe/S3Mock) to run
integration tests against a real S3-compatible server.

```bash
./mvnw test
```

---

## 🗺️ Roadmap

- [x] `S3ItemWriter`
- [ ] `S3ItemReader`
- [ ] Support for additional output formats (JSON, Parquet, ...)

---

## 🤝 Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'Add my feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.