# 系统依赖说明

为确保 PDF 表格与 OCR 功能正常运行，需要在系统层面安装以下组件：

- **Java Runtime Environment (JRE)**：`tabula-py` 依赖 Java 来调用 Tabula。
- **Poppler**：`pdf2image` 将 PDF 渲染为图像时需要 Poppler。
- **Tesseract OCR**：`pytesseract` 需要本地的 Tesseract 可执行程序。

请根据操作系统安装相应的系统包，并确保这些可执行文件可以在 PATH 中访问。
