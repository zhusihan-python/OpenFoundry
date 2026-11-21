# BIFF8 reader

The XLS decoder and value model are adapted from
[go-excel-reader v0.3.0](https://github.com/jloor/go-excel-reader/tree/5b56c14a4b071bd1507f0f2412f5a780d9f4b95a)
under the included MIT license. The source subset uses the Go standard
library. Local pinned-input changes check the file, workbook globals,
type and encryption before ordinary cell decoding. Display-format helpers are
excluded because the pinned dataset path retains stored values. Keep the license and
upstream revision with future updates.
