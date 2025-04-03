# Clean Architecture Refactoring Plan

## Overview
This document outlines the plan to refactor the codebase following clean architecture principles, moving away from client-specific behaviors to domain-driven, use-case-based implementations.

## Current Status
The codebase currently uses a client-specific approach with:
- Client-specific behavior files (BS, 2E, etc.)
- Custom merge logic per client
- Mixed business logic and data transformation
- Tight coupling to Spark DataFrames
- Limited separation of concerns

## Target Architecture

### 1. Core Domain Layer (`src/core/`)
```
src/core/
├── domain/
│   ├── entities/
│   │   ├── event.py           # Core event entity
│   │   ├── vehicle.py         # Vehicle entity
│   │   └── calculation_result.py  # Calculation results
│   └── value_objects/
│       ├── fuel_consumption.py    # Fuel consumption metrics
│       └── time_period.py         # Time-related values
├── use_cases/
│   ├── calculations/
│   │   ├── reduced_engine_taxi.py  # RETI/RETO calculations
│   │   ├── efficient_flight.py     # Flight efficiency
│   │   └── optimal_fuel_load.py    # Fuel optimization
│   └── insights/
│       ├── fuel_efficiency.py      # Fuel efficiency metrics
│       └── performance_metrics.py   # Performance calculations
└── ports/
    ├── repositories.py         # Repository interfaces
    └── services.py            # Service interfaces
```

### 2. Infrastructure Layer (`src/infrastructure/`)
```
src/infrastructure/
├── persistence/
│   ├── spark/
│   │   └── dataframe_repository.py  # Spark implementation
│   └── repositories/
│       └── event_repository.py      # Event data access
├── services/
│   └── calculation_service.py       # Calculation implementations
└── adapters/
    └── spark_adapter.py             # Spark adaptation layer
```

### 3. Application Layer (`src/application/`)
```
src/application/
├── services/
│   ├── calculation_orchestrator.py  # Orchestrates calculations
│   └── event_processor.py           # Processes events
└── interfaces/
    └── calculation_interface.py     # Public interfaces
```

### 4. Presentation Layer (`src/tasks/`)
```
src/tasks/
├── calculation/
│   ├── task.py                     # Task definitions
│   └── config.py                   # Task configuration
└── common/
    └── task_base.py               # Common task functionality
```

## Implementation Plan

### Phase 1: Core Domain (Week 1-2)
1. Create domain entities:
   ```python
   @dataclass
   class Event:
       id: str
       vehicle: Vehicle
       timestamp: datetime
       fuel_consumption: FuelConsumption
       taxi_time: TimePeriod
   ```

2. Define value objects:
   ```python
   @dataclass(frozen=True)
   class FuelConsumption:
       amount: Decimal
       unit: str
       
       def is_valid(self) -> bool:
           return self.amount > 0
   ```

3. Implement use case interfaces:
   ```python
   class CalculationUseCase(ABC):
       @abstractmethod
       def execute(self, event_id: str) -> CalculationResult:
           pass
   ```

### Phase 2: Infrastructure (Week 3-4)
1. Create Spark adapter:
   ```python
   class SparkEventRepository(EventRepository):
       def __init__(self, spark_session):
           self.spark = spark_session

       def get_event(self, event_id: str) -> Event:
           df = self.spark.sql(f"SELECT * FROM events WHERE id = '{event_id}'")
           return self._map_to_entity(df.first())
   ```

2. Implement repositories:
   ```python
   class EventRepository(ABC):
       @abstractmethod
       def get_event(self, event_id: str) -> Event:
           pass

       @abstractmethod
       def save_calculation(self, event_id: str, result: CalculationResult):
           pass
   ```

### Phase 3: Use Cases (Week 5-6)
1. Implement calculation use cases:
   ```python
   class ReducedEngineTaxiUseCase:
       def __init__(self, event_repository: EventRepository):
           self.event_repository = event_repository

       def execute(self, event_id: str) -> CalculationResult:
           event = self.event_repository.get_event(event_id)
           
           if not self._is_eligible(event):
               return CalculationResult.not_eligible(
                   reason=self._get_ineligibility_reason(event)
               )
               
           savings = self._calculate_fuel_savings(event)
           return CalculationResult.success(savings=savings)

       def _is_eligible(self, event: Event) -> bool:
           return (
               not event.is_covid_period() and
               event.taxi_time.is_within_limits() and
               event.fuel_consumption.is_valid()
           )
   ```

2. Add validation and business rules:
   ```python
   class ValidationService:
       def validate_taxi_time(self, time: TimePeriod) -> bool:
           return (
               time.duration > MIN_TAXI_TIME and
               time.duration < MAX_TAXI_TIME
           )
   ```

### Phase 4: Application Layer (Week 7-8)
1. Create orchestration service:
   ```python
   class CalculationOrchestrator:
       def __init__(self, use_cases: Dict[str, BaseUseCase]):
           self.use_cases = use_cases

       def process_event(
           self, 
           event_id: str, 
           calculation_types: List[str]
       ) -> Dict[str, CalculationResult]:
           results = {}
           for calc_type in calculation_types:
               use_case = self.use_cases[calc_type]
               results[calc_type] = use_case.execute(event_id)
           return results
   ```

2. Implement task interfaces:
   ```python
   class CalculationTask:
       def __init__(self, orchestrator: CalculationOrchestrator):
           self.orchestrator = orchestrator

       def run(self, config: TaskConfig):
           events = self._load_events(config)
           for event in events:
               results = self.orchestrator.process_event(
                   event.id,
                   config.calculation_types
               )
               self._save_results(event.id, results)
   ```

### Phase 5: Migration (Week 9-10)
1. Create parallel implementations
2. Validate results match existing code
3. Gradually replace old implementations
4. Update tests and documentation

## Benefits
1. **Cleaner Architecture**
   - Clear separation of concerns
   - Domain-driven design
   - Easier to test and maintain

2. **Better Abstraction**
   - Domain entities instead of raw DataFrames
   - Business logic separated from data access
   - Reusable calculation components

3. **Improved Maintainability**
   - Each calculation is a standalone use case
   - Clear interfaces between layers
   - Better error handling and validation

4. **Enhanced Testability**
   - Domain logic can be tested without Spark
   - Mock repositories for faster tests
   - Clear test boundaries

## Migration Strategy
1. Start with one calculation type (e.g., RETI)
2. Create parallel implementation
3. Validate results match
4. Gradually migrate other calculations
5. Remove old code once verified

## Next Steps
1. Set up new directory structure
2. Create core domain entities
3. Implement first use case
4. Add tests for new implementation
5. Begin parallel testing with existing code

## Notes
- Keep backward compatibility during migration
- Document all changes in CHANGELOG.md
- Update API documentation as we progress
- Regular validation of calculation results

## Current Analysis Needed

### Behavior Analysis
Need to analyze existing behaviors to identify:
1. Common patterns across different DDS codes
2. Business rules that can be extracted into domain entities
3. Data transformations that can be moved to adapters
4. Validation rules that can be standardized

### Files to Review:
- src/tasks/dds/vs/behaviours.py (RETI/RETO implementation)
- src/tasks/dds/common_behaviours.py (base calculations)
- src/tasks/dds/common_merge.py (merge logic)
- All DDS-specific mapping files

### Questions to Answer:
1. What are the core domain concepts across all behaviors?
2. Which calculations are truly DDS-specific vs. reusable?
3. How can we maintain Spark performance while abstracting DataFrame operations?
4. What is the minimal interface needed for the first use case (RETI)?

## Immediate Next Steps

### Day 1: Analysis & Planning
1. Review RETI/RETO implementation in detail
2. Document all business rules and validations
3. Identify common patterns in data transformations
4. Map out core domain entities needed

### Day 2: Core Domain Setup
1. Create initial directory structure
2. Implement basic Event and Vehicle entities
3. Define first value objects (FuelConsumption, TimePeriod)
4. Write tests for core domain logic

### Day 3: First Use Case
1. Implement ReducedEngineTaxiUseCase
2. Create Spark adapter for data access
3. Set up basic orchestration
4. Add parallel implementation alongside existing code

## Progress Tracking
- [ ] Complete behavior analysis
- [ ] Document core domain concepts
- [ ] Create initial directory structure
- [ ] Implement first domain entities
- [ ] Set up test infrastructure
- [ ] Create first use case implementation

## Open Questions
1. How to handle Spark DataFrame operations efficiently in the new architecture?
2. Best approach for gradual migration without disrupting existing functionality?
3. How to maintain performance while adding abstraction layers?
4. Strategy for handling DDS-specific configurations in the new architecture?

## Reference Materials
- Current VS behaviors implementation
- Common behaviors base class
- Mapping files for different DDS codes
- Existing test suite structure 