<h1>Project Overview</h1>

<p>
This project implements a multi-stage pipeline that processes a stream of JSON data, scales it to significant sizes and performs high-intensity SHA calculations across distributed parallel C++ and Python environments. RabbitMQ is used for communication between environments.
</p>


<h2>C++ Components (The Producer/Consumer)</h2>
<ul>
    <li><strong>OpenSSL:</strong> Low-level SHA512 cryptographic calculations.</li>
    <li><strong>Intel OneTBB:</strong> Task-based parallelism and pipeline orchestration.</li>
    <li><strong>simdjson & nlohmann/json:</strong> High-performance JSON parsing.</li>
    <li><strong>SimpleAmqpClient:</strong> C++ wrapper for RabbitMQ communication.</li>
</ul>

<h2>Python Components (The Distributed Workers)</h2>
<ul>
    <li><strong>Ray:</strong> Distributed actor model for massive parallelism across processes.</li>
    <li><strong>Hashlib:</strong> Optimized Python SHA256 implementation.</li>
    <li><strong>JSON:</strong> Data serialization for communication.</li>
</ul>

<h3>Additional requirements:</h3>
<ul>
    <li>CMake</li>
    <li>vcpkg</li>
</ul>

<p>
The <code>data.json</code> file contains 1200 lines of strings that have 10000 symbols (~10 kB). In order to achieve the requirement (one Python worker must calculate all SHA256 for at least 60 seconds) strings are repeated to increase their size (<code>scale_factor=10000</code>). So, before SHA512 and SHA256 strings are made into ~100 MB texts while communication using RabbitMQ still uses the initial smaller strings.
</p>

<p>
Huge strings are not generated in the beginning because communication over RabbitMQ with such huge files becomes a bottleneck. When it comes to Python, Ray actors receive the data, scale it to 100 MB on their side, and compute SHA256. There is a separate consumer thread in C++ that aggregates results and confirms processing.
</p>

<p>
Testing was conducted on an Intel i5-1235U (10 Physical Cores / 12 Logical Cores) with 32 GB RAM. Results are given in the table below.
</p>

<img width="900" height="434" alt="Parallel results" src="https://github.com/user-attachments/assets/ee4c1418-bf21-4841-875d-5d1de619b21e" />



<p>
Parallel benchmarks revealed that the optimal performance was achieved when the total number of threads/processes exceeded the logical core count (15 units total: 9 C++ threads + 6 Ray actors).
</p>

<p>
That is due to I/O operations: C++ consumer thread is time blocking, meaning that most of the time it sits idle waiting for results from Python. Context switching allows to have bigger number of threads/processes – by switching working threads/processes on a core it makes sure that no core is left being idle and not used to its maximum.
</p>
