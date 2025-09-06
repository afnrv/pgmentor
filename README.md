# pgmentor

A comprehensive CLI tool for PostgreSQL query analysis and optimization that provides intelligent recommendations for database performance tuning.

## Features

### 🔍 Query Analysis
- **Query Performance Analysis**: Analyze individual SQL queries with detailed execution statistics
- **Lock Analysis**: Detect and analyze database locks, including blocking relationships
- **AI-Powered Optimization**: Get intelligent query optimization suggestions using DeepSeek AI
- **Statistics Analysis**: Analyze `pg_stat_statements` for slow queries and performance bottlenecks

### ⚙️ Configuration Optimization
- **Parameter Recommendations**: Get tailored PostgreSQL configuration recommendations based on your system
- **Profile-Based Tuning**: Support for OLTP and OLAP workload profiles
- **System-Aware**: Automatically detects system resources (RAM, CPU, storage type)
- **Priority-Based Suggestions**: Recommendations ranked by impact and implementation priority

### 📊 Database Health Monitoring
- **Comprehensive Health Checks**: 18 different database health metrics and analyses
- **Performance Bottlenecks**: Identify large tables, unused indexes, dead tuples, and more
- **Replication Monitoring**: Check replication lag and slot status
- **Resource Usage**: Monitor temp files, XID freeze age, and memory usage

## Installation

### From Source
```bash
git clone https://github.com/yourusername/pgmentor.git
cd pgmentor
pip install -e .
```

### From virtual environment
```bash
# Create virtual environment
python3 -m venv ~/pgmentor-venv

# Activate it
source ~/pgmentor-venv/bin/activate

# Install wheel
pip install ~/pgmentor/pgmentor/dist/pgmentor-0.1.0-py3-none-any.whl
```

## Dependencies

- Python 3.9+
- psycopg2-binary
- openai (for AI-powered query optimization)

## Configuration

### Environment Variables
Set your DeepSeek API key for AI-powered query optimization:
```bash
export API_KEY_DEEPSEEK="your_deepseek_api_key"
```

## Usage

### Basic Syntax
```bash
pgmentor [OPTIONS] -ci <connection_string>
```

### Connection String
Use PostgreSQL connection string format:
```bash
pgmentor -ci "host=localhost port=5432 dbname=mydb user=myuser password=mypass"
```

### Command Options

#### Configuration Analysis
Get PostgreSQL configuration recommendations:
```bash
# Basic configuration analysis
pgmentor -ci "host=localhost dbname=mydb user=myuser" -c

# OLAP profile with output file
pgmentor -ci "host=localhost dbname=mydb user=myuser" -c -p olap -o recommendations.sql

# OLTP profile (default)
pgmentor -ci "host=localhost dbname=mydb user=myuser" -c -p oltp
```

#### Query Analysis
Analyze and optimize individual queries:
```bash
# Analyze a specific query
pgmentor -ci "host=localhost dbname=mydb user=myuser" -q "SELECT * FROM users WHERE id = 1"

# Save analysis to file
pgmentor -ci "host=localhost dbname=mydb user=myuser" -q "SELECT * FROM users WHERE id = 1" -o query_analysis.txt
```

#### Statistics Analysis
Analyze slow queries from pg_stat_statements:
```bash
# Analyze query statistics
pgmentor -ci "host=localhost dbname=mydb user=myuser" -a

# Save statistics analysis
pgmentor -ci "host=localhost dbname=mydb user=myuser" -a -o stats_analysis.txt
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `-ci, --conninfo` | PostgreSQL connection string (required) |
| `-c, --configure` | Show PostgreSQL configuration recommendations |
| `-q, --query` | Query to analyze and optimize |
| `-a, --analyze-stats` | Analyze query statistics from pg_stat_statements |
| `-p, --profile` | Profile to use: `oltp` (default) or `olap` |
| `-o, --out-file` | Write output to file |
| `-v, --version` | Show version information |

## What pgmentor Analyzes

### Configuration Recommendations
- **Memory Settings**: shared_buffers, effective_cache_size, work_mem, maintenance_work_mem
- **WAL Settings**: wal_buffers, min_wal_size, max_wal_size, wal_compression
- **Checkpoint Settings**: checkpoint_timeout, checkpoint_completion_target
- **I/O Settings**: random_page_cost, effective_io_concurrency
- **Autovacuum Settings**: naptime, cost limits, worker counts
- **Parallel Processing**: max_parallel_workers, max_parallel_workers_per_gather
- **Logging**: query duration logging, checkpoint logging, autovacuum logging

### Database Health Checks
1. **PostgreSQL Parameters** - Configuration recommendations
2. **Checkpoint & Background Writer** - Checkpoint performance metrics
3. **Large Tables** - Tables >20GB for potential partitioning
4. **HOT Updates** - Tables with low HOT update percentage
5. **Sequential vs Index Scans** - Tables with excessive sequential scans
6. **Duplicate Indexes** - Redundant indexes consuming space
7. **Foreign Keys without Indexes** - Missing indexes on FK columns
8. **Tables without Primary Keys** - Large tables missing primary keys
9. **Unused Indexes** - Indexes that are never used
10. **Dead Tuples/Bloat** - Tables with high dead tuple percentage
11. **Temporary Files** - Database temp file usage
12. **XID Freeze Age** - Transaction ID age monitoring
13. **Wait Events** - Current database wait events
14. **Replication Lag** - Replication slot status and lag
15. **Extensions** - Installed PostgreSQL extensions
16. **HugePages/Shared Memory** - Memory configuration
17. **Archiving/WAL Size** - WAL and archiving configuration

### Query Analysis Features
- **Execution Plan Analysis**: Detailed cost and timing estimates
- **Lock Detection**: Identifies locks held and blocking relationships
- **AI Optimization**: Intelligent suggestions for query improvement
- **Index Recommendations**: Suggests optimal indexes for query performance
- **Resource Usage**: Memory and I/O impact analysis

## Example Output

### Configuration Recommendations
```
============================================================
                    1) PG parameters
============================================================
parameter                       | current     | recommended | action | why                    | priority | speedup
shared_buffers                  | 128MB       | 1024MB      | restart| ≈25 % RAM             | medium   | 5%
work_mem                        | 4MB         | 12MB        | session| 1.5×p90 sort          | high     | 10%
random_page_cost                | 4           | 1.1         | session| SSD optimization      | medium   | 3%
```

### Query Analysis
```
============================================================
                      Query Statistics
============================================================
Total cost: 1250.50
Estimated time: 0.125050 s
Estimated rows: 1000
Estimated row size: 64 bytes
Estimated volume: 64000 bytes
work_mem: 4MB
seq_page_cost: 1, random_page_cost: 4

============================================================
                           Locks
============================================================
Locks held by PID 12345:
 [GRANTED] AccessShareLock on relation → table='users'

============================================================
                        Optimization
============================================================
Consider adding an index on the WHERE clause column:
CREATE INDEX idx_users_email ON users(email);

This will reduce the sequential scan cost and improve query performance...
```

## System Requirements

- **Operating System**: Linux (for system metrics collection)
- **PostgreSQL**: Version 12+ (some features require newer versions)
- **Extensions**: pg_stat_statements (recommended for statistics analysis)
- **Permissions**: Database user needs access to system catalogs and statistics views

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Author

**Artem Finarev** - [artyom310300@gmail.com](mailto:artyom310300@gmail.com)

## Support

For issues, feature requests, or questions:
- Open an issue on GitHub
- Contact the author via email

---

*pgmentor - Making PostgreSQL optimization accessible and intelligent*