import logging
import sys
from typing import List, Union, Tuple, Iterator
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from dataclasses import dataclass
from enum import Enum
import time
from pathlib import Path
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('addition_service.log'),a
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OperationStatus(Enum):
    """Status enumeration for operations."""
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL = "partial"


class NumberType(Enum):
    """Supported number types."""
    INTEGER = "integer"
    FLOAT = "float"
    DECIMAL = "decimal"


@dataclass
class OperationResult:
    """Data class for operation results with metadata."""
    status: OperationStatus
    result: Union[Decimal, None]
    error_message: str = ""
    processed_count: int = 0
    failed_count: int = 0
    execution_time: float = 0.0
    number_types: dict = None
    source: str = ""


class NumberValidator:
    """Handles input validation for multiple number types."""
    
    _cache = {}
    MAX_CACHE_SIZE = 10000
    
    @classmethod
    def validate(cls, value: str) -> Tuple[bool, Union[Decimal, str], NumberType]:
        """
        Validate and convert string to Decimal with type detection.
        Returns: (is_valid, result_or_error_message, number_type)
        """
        cache_key = value.strip()
        if cache_key in cls._cache:
            return cls._cache[cache_key]
        
        try:
            value_clean = value.strip()
            
            # Try to parse as number
            if not value_clean or value_clean.lower() in ['nan', 'inf', '-inf']:
                raise ValueError("Invalid numeric value")
            
            num = Decimal(value_clean)
            
            # Detect number type
            if '.' not in value_clean and 'e' not in value_clean.lower():
                num_type = NumberType.INTEGER
            elif 'e' in value_clean.lower():
                num_type = NumberType.FLOAT
            else:
                num_type = NumberType.DECIMAL
            
            result = (True, num, num_type)
            
        except (InvalidOperation, ValueError) as e:
            result = (False, f"Invalid number: {value_clean}", None)
            logger.warning(f"Validation failed for input: {value_clean}")
        
        # Cache eviction
        if len(cls._cache) >= cls.MAX_CACHE_SIZE:
            cls._cache.clear()
        
        cls._cache[cache_key] = result
        return result


class InputSource(ABC):
    """Abstract base class for input sources."""
    
    @abstractmethod
    def read(self) -> Iterator[str]:
        """Read numbers from source."""
        pass
    
    @abstractmethod
    def validate_source(self) -> Tuple[bool, str]:
        """Validate source accessibility."""
        pass


class ListSource(InputSource):
    """Input source from Python list."""
    
    def __init__(self, numbers: List[Union[str, int, float]]):
        self.numbers = numbers
    
    def validate_source(self) -> Tuple[bool, str]:
        if not isinstance(self.numbers, (list, tuple)):
            return False, "Input must be a list or tuple"
        if not self.numbers:
            return False, "List is empty"
        return True, "Valid"
    
    def read(self) -> Iterator[str]:
        for num in self.numbers:
            yield str(num)


class FileSource(InputSource):
    """Input source from CSV file."""
    
    def __init__(self, filepath: str, column_index: int = 0, skip_header: bool = True):
        self.filepath = Path(filepath)
        self.column_index = column_index
        self.skip_header = skip_header
    
    def validate_source(self) -> Tuple[bool, str]:
        if not self.filepath.exists():
            return False, f"File not found: {self.filepath}"
        if not self.filepath.suffix.lower() in ['.csv', '.txt']:
            return False, "Only CSV and TXT files supported"
        if not self.filepath.stat().st_size > 0:
            return False, "File is empty"
        return True, "Valid"
    
    def read(self) -> Iterator[str]:
        try:
            with open(self.filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                
                if self.skip_header:
                    next(reader, None)
                
                for row_idx, row in enumerate(reader):
                    try:
                        if self.column_index < len(row):
                            value = row[self.column_index].strip()
                            if value:
                                yield value
                        else:
                            logger.warning(f"Row {row_idx}: Column index {self.column_index} out of range")
                    except Exception as e:
                        logger.warning(f"Error reading row {row_idx}: {e}")
                        
        except IOError as e:
            logger.error(f"File read error: {e}")
            raise


class AdditionService:
    """Production-grade addition service supporting multiple input sources."""
    
    def __init__(self, precision: int = 2, timeout: float = 30.0):
        self.precision = precision
        self.timeout = timeout
        self.operations_count = 0
    
    def add_from_source(self, source: InputSource) -> OperationResult:
        """
        Add numbers from any input source with comprehensive validation.
        """
        start_time = time.time()
        
        # Validate source
        is_valid, msg = source.validate_source()
        if not is_valid:
            logger.error(f"Source validation failed: {msg}")
            return OperationResult(
                status=OperationStatus.FAILURE,
                result=None,
                error_message=msg,
                execution_time=time.time() - start_time,
                source=source.__class__.__name__
            )
        
        total = Decimal('0')
        processed = 0
        failed = 0
        errors = []
        type_counts = {t.value: 0 for t in NumberType}
        
        try:
            for idx, value in enumerate(source.read()):
                # Timeout check
                if time.time() - start_time > self.timeout:
                    raise TimeoutError(f"Operation timeout after {self.timeout}s")
                
                # Validate number
                is_valid, result, num_type = NumberValidator.validate(value)
                
                if is_valid:
                    total += result
                    processed += 1
                    if num_type:
                        type_counts[num_type.value] += 1
                else:
                    failed += 1
                    errors.append(f"Index {idx}: {result}")
            
            # Round result to precision (for financial use)
            if total is not None:
                total = total.quantize(
                    Decimal(10) ** -self.precision,
                    rounding=ROUND_HALF_UP
                )
            
            # Determine status
            if failed == 0:
                status = OperationStatus.SUCCESS
            elif processed == 0:
                status = OperationStatus.FAILURE
                total = None
            else:
                status = OperationStatus.PARTIAL
            
            execution_time = time.time() - start_time
            self.operations_count += 1
            
            result_obj = OperationResult(
                status=status,
                result=total,
                error_message="; ".join(errors[:5]) if errors else "",
                processed_count=processed,
                failed_count=failed,
                execution_time=execution_time,
                number_types=type_counts,
                source=source.__class__.__name__
            )
            
            logger.info(f"Operation {self.operations_count}: Source={source.__class__.__name__}, "
                       f"Status={status.value}, Processed={processed}, Failed={failed}, "
                       f"Time={execution_time:.4f}s")
            
            return result_obj
            
        except TimeoutError as e:
            logger.error(f"Timeout: {e}")
            return OperationResult(
                status=OperationStatus.FAILURE,
                result=None,
                error_message=str(e),
                execution_time=time.time() - start_time,
                source=source.__class__.__name__
            )
        except Exception as e:
            logger.error(f"Error: {type(e).__name__}: {e}")
            return OperationResult(
                status=OperationStatus.FAILURE,
                result=None,
                error_message=f"{type(e).__name__}: {str(e)}",
                execution_time=time.time() - start_time,
                source=source.__class__.__name__
            )
    
    def add_numbers(self, numbers: List[Union[str, int, float]]) -> OperationResult:
        """Convenience method for list input."""
        source = ListSource(numbers)
        return self.add_from_source(source)
    
    def add_from_csv(self, filepath: str, column_index: int = 0, skip_header: bool = True) -> OperationResult:
        """Convenience method for CSV input."""
        source = FileSource(filepath, column_index, skip_header)
        return self.add_from_source(source)


def print_result(result: OperationResult):
    """Clean, minimal output formatting."""
    print(f"\n{'='*50}")
    if result.result is not None:
        print(f"Sum: {result.result}")
    else:
        print(f"Status: {result.status.value.upper()}")
    if result.error_message:
        print(f"Errors: {result.error_message}")
    print(f"{'='*50}\n")


def add_two_numbers():
    """Simple function to add two numbers with validation."""
    service = AdditionService(precision=2)
    
    try:
        num1_str = input("Enter first number (integer, float, or decimal): ").strip()
        num2_str = input("Enter second number (integer, float, or decimal): ").strip()
        
        result = service.add_numbers([num1_str, num2_str])
        print_result(result)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        print(f"Error: {e}")


def interactive_mode():
    """Interactive CLI for testing."""
    service = AdditionService(precision=2)
    
    while True:
        try:
            print("\n=== Addition Service (Multi-Source) ===")
            print("1. Add two numbers")
            print("2. Add from list")
            print("3. Exit")
            
            choice = input("Enter choice (1-4): ").strip()
            
            if choice == "1":
                add_two_numbers()
                
            elif choice == "2":
                user_input = input("Enter numbers (comma-separated): ").strip()
                numbers = [x.strip() for x in user_input.split(',')]
                result = service.add_numbers(numbers)
                print_result(result)
                
                
            elif choice == "3":
                print("Exiting...")
                break
            else:
                print("Invalid choice")
                
        except KeyboardInterrupt:
            print("\n\nProgram interrupted")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            print(f"Error: {e}")


if __name__ == "__main__":
    interactive_mode()