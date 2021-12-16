//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  // 构造函数，分配内存
  Matrix(int rows, int cols) {
    this->linear_ = new int[rows * cols];
    this->rows_ = rows;
    this->cols_ = cols;
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  // 销毁一个矩阵实例，释放内存
  virtual ~Matrix() { delete[] this->linear_; }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    // 调用虚函数的构造函数
    // Matrix::Matrix(rows, cols);
    // 为 RowMatrix 分配内存
    this->data_ = new int *[rows];
    for (int i = 0; i < rows; i++) {
      // 将每行的指针指向对应的地址
      this->data_[i] = this->linear_ + (i * cols);
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    // throw NotImplementedException{"RowMatrix::GetElement() not implemented."};
    // return this->data_[i][j];
    if (i >= this->rows_ || i < 0 || j >= this->cols_ || j < 0) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "RowMatrix::GetElement() out of range.");
    } else {
      return this->data_[i][j];
    }
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i >= this->rows_ || i < 0 || j >= this->cols_ || j < 0) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "RowMatrix::SetElement() out of range.");
    } else {
      this->data_[i][j] = val;
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    // throw NotImplementedException{"RowMatrix::FillFrom() not implemented."};
    size_t size = this->cols_ * this->rows_;
    if (size != source.size()) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "RowMatrix::FillFrom() out of range.");
    } else {
      for (size_t i = 0; i < source.size(); i++) {
        this->linear_[i] = source[i];
      }
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() {
    delete[] this->data_;
    // Matrix::~Matrix();
  }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    // return std::unique_ptr<RowMatrix<T>>(nullptr);

    if (matrixA->GetRowCount() == matrixB->GetRowCount() && matrixA->GetColumnCount() && matrixB->GetColumnCount()) {
      int rows = matrixA->GetRowCount();
      int cols = matrixB->GetColumnCount();
      std::unique_ptr<RowMatrix<T>> sum_matrix(new RowMatrix<T>(rows, cols));
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          sum_matrix->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
        }
      }
      return sum_matrix;
    } else {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    // return std::unique_ptr<RowMatrix<T>>(nullptr);
    int arows = matrixA->GetRowCount();
    int acols = matrixA->GetColumnCount();
    int brows = matrixB->GetRowCount();
    int bcols = matrixB->GetColumnCount();
    if (acols == brows) {
      std::unique_ptr<RowMatrix<T>> mul_matrix(new RowMatrix<T>(arows, bcols));
      for (int i = 0; i < arows; i++) {
        for (int j = 0; j < bcols; j++) {
          int temp = 0;
          for (int k = 0; k < acols; k++) {
            temp += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
          }

          mul_matrix->SetElement(i, j, temp);
        }
      }
      return mul_matrix;
    } else {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    // return std::unique_ptr<RowMatrix<T>>(nullptr);
    int arows = matrixA->GetRowCount();
    int acols = matrixA->GetColumnCount();
    int brows = matrixB->GetRowCount();
    int bcols = matrixB->GetColumnCount();
    int crows = matrixC->GetRowCount();
    int ccols = matrixC->GetColumnCount();
    if (acols == brows && arows == crows && bcols == ccols) {
      std::unique_ptr<RowMatrix<T>> gmm_matrix(new RowMatrix<T>(arows, bcols));
      for (int i = 0; i < arows; i++) {
        for (int j = 0; j < bcols; j++) {
          int temp = 0;
          for (int k = 0; k < acols; k++) {
            temp += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
          }
          gmm_matrix->SetElement(i, j, temp + matrixC->GetElement(i, j));
        }
      }
      return gmm_matrix;
    } else {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
  }
};
}  // namespace bustub
