CREATE DATABASE Fedex_Operations_DB

CREATE TABLE DimDate (
    DateKey              INT         NOT NULL PRIMARY KEY, -- YYYYMMDD
    Date                 DATE        NOT NULL,
    FullDateLabel        VARCHAR(20) NOT NULL, -- '2025-10-01'
    CalendarYear         INT         NOT NULL,
    CalendarQuarter      TINYINT     NOT NULL,
    CalendarQuarterName  VARCHAR(6)  NOT NULL, -- 'Q1'
    CalendarMonth        TINYINT     NOT NULL,
    CalendarMonthName    VARCHAR(10) NOT NULL,
    CalendarWeek         TINYINT     NOT NULL,
    DayOfMonth           TINYINT     NOT NULL,
    DayOfWeek            TINYINT     NOT NULL, -- 1=Mon
    DayOfWeekName        VARCHAR(10) NOT NULL,
    IsWeekend            BIT         NOT NULL,
    FiscalYear           INT         NOT NULL,
    FiscalQuarter        TINYINT     NOT NULL
);

CREATE TABLE DimCustomer (
    CustomerKey          INT          IDENTITY(1,1) PRIMARY KEY,
    SourceCustomerID     VARCHAR(50)  NOT NULL,
    CustomerNameHash     VARCHAR(100) NULL, -- anonymized/hashed
    CustomerSegment      VARCHAR(50)  NULL, -- 'SMB','Enterprise','Consumer'
    Industry             VARCHAR(50)  NULL, -- 'Retail','Manufacturing', etc.
    IsActive             BIT          NOT NULL DEFAULT 1
);

CREATE TABLE DimLocation (
    LocationKey          INT          IDENTITY(1,1) PRIMARY KEY,
    LocationCode         VARCHAR(20)  NOT NULL, -- e.g., 'ATL_HUB01'
    LocationName         VARCHAR(100) NOT NULL,
    City                 VARCHAR(100) NULL,
    StateProvince        VARCHAR(100) NULL,
    Country              VARCHAR(100) NULL,
    Region               VARCHAR(50)  NULL, -- 'Southeast','Midwest','Northeast','West','International'
    LocationType         VARCHAR(20)  NULL  -- 'Hub','Depot','CustomerCity','Airport'
);

CREATE TABLE DimDriver (
    DriverKey            INT          IDENTITY(1,1) PRIMARY KEY,
    DriverCode           VARCHAR(20)  NOT NULL,
    DriverName           VARCHAR(100) NULL,
    HireDate             DATE         NULL,
    TerminationDate      DATE         NULL,
    EmploymentStatus     VARCHAR(20)  NULL, -- 'Active','Terminated','On Leave'
    ContractorFlag       BIT          NULL  -- 1=contractor, 0=employee
);

CREATE TABLE DimRoute (
    RouteKey             INT          IDENTITY(1,1) PRIMARY KEY,
    RouteCode            VARCHAR(20)  NOT NULL,
    RouteName            VARCHAR(100) NULL,
    Region               VARCHAR(50)  NULL, -- should align with DimLocation.Region
    AverageDistanceKm    DECIMAL(10,2) NULL,
    RouteType            VARCHAR(20)  NULL, -- 'Urban','Rural','Suburban','International'
    IsActive             BIT          NOT NULL DEFAULT 1
);

CREATE TABLE DimService (
    ServiceKey           INT          IDENTITY(1,1) PRIMARY KEY,
    ServiceCode          VARCHAR(10)  NOT NULL, -- 'STD','EXP','OVN'
    ServiceLevel         VARCHAR(20)  NOT NULL, -- 'Standard','Express','Overnight'
    PriorityFlag         VARCHAR(10)  NOT NULL, -- 'Normal','High'
    ProductType          VARCHAR(50)  NULL      -- 'Parcel','Document','FreightLite'
);

CREATE TABLE DimExceptionType (
    ExceptionTypeKey     INT          IDENTITY(1,1) PRIMARY KEY,
    ExceptionCode        VARCHAR(20)  NOT NULL, -- 'WEATHER','ADDR','CNA','CUSTOMS','DAMAGE','OTHER'
    ExceptionCategory    VARCHAR(50)  NOT NULL, -- 'Weather','Address Issue', etc.
    Severity             VARCHAR(20)  NULL,     -- 'Low','Medium','High'
    IsCustomerVisible    BIT          NULL      -- visible to tracking / SLA impact
);

CREATE TABLE FactShipment (
    ShipmentKey              INT          IDENTITY(1,1) PRIMARY KEY,
    ShipmentID               VARCHAR(50)  NOT NULL, -- source system ID
    TrackingNumber           VARCHAR(50)  NOT NULL,

    -- Date FK keys (role-playing DimDate)
    ShipDateKey              INT          NOT NULL,
    PromisedDateKey          INT          NULL,
    ActualDeliveryDateKey    INT          NULL,

    -- Dimensions
    CustomerKey              INT          NULL,
    OriginLocationKey        INT          NULL,
    DestinationLocationKey   INT          NULL,
    DriverKey                INT          NULL,
    RouteKey                 INT          NULL,
    ServiceKey               INT          NULL,

    -- Status & exception
    ShipmentStatus           VARCHAR(20)  NOT NULL, -- 'Delivered','In Transit','Delayed','Exception','Returned'
    HasExceptionFlag         BIT          NOT NULL DEFAULT 0,
    PrimaryExceptionTypeKey  INT          NULL,

    -- Operational metrics
    NumberOfPackages         INT          NULL,
    WeightKg                 DECIMAL(10,2) NULL,
    VolumeCubicM             DECIMAL(10,3) NULL,
    DistanceKm               DECIMAL(10,2) NULL,

    -- Financials
    ShippingFee              DECIMAL(10,2) NULL,
    FuelSurcharge            DECIMAL(10,2) NULL,
    OtherSurcharge           DECIMAL(10,2) NULL,
    TotalRevenue             DECIMAL(10,2) NULL,
    EstimatedCost            DECIMAL(10,2) NULL,

    -- Derived fields (ETL/Power Query)
    DeliveryStatusFlag       VARCHAR(20)  NULL, -- 'On Time','Late','In Transit'
    TransitDays              INT          NULL,
    DelayDays                INT          NULL
);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_ShipDate
    FOREIGN KEY (ShipDateKey) REFERENCES DimDate(DateKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_PromisedDate
    FOREIGN KEY (PromisedDateKey) REFERENCES DimDate(DateKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_ActualDate
    FOREIGN KEY (ActualDeliveryDateKey) REFERENCES DimDate(DateKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_Customer
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_Origin
    FOREIGN KEY (OriginLocationKey) REFERENCES DimLocation(LocationKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_Destination
    FOREIGN KEY (DestinationLocationKey) REFERENCES DimLocation(LocationKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_Driver
    FOREIGN KEY (DriverKey) REFERENCES DimDriver(DriverKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_Route
    FOREIGN KEY (RouteKey) REFERENCES DimRoute(RouteKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_Service
    FOREIGN KEY (ServiceKey) REFERENCES DimService(ServiceKey);

ALTER TABLE FactShipment
ADD CONSTRAINT FK_FactShipment_PrimaryException
    FOREIGN KEY (PrimaryExceptionTypeKey) REFERENCES DimExceptionType(ExceptionTypeKey);

CREATE TABLE FactExceptionEvent (
    ExceptionEventKey       INT          IDENTITY(1,1) PRIMARY KEY,
    ShipmentKey             INT          NOT NULL,
    ExceptionTypeKey        INT          NOT NULL,
    EventDateKey            INT          NOT NULL,
    EventTimestamp          DATETIME2(0) NOT NULL,
    EventLocationKey        INT          NULL,
    ExceptionDescription    VARCHAR(255) NULL
);

ALTER TABLE FactExceptionEvent
ADD CONSTRAINT FK_ExceptionEvent_Shipment
    FOREIGN KEY (ShipmentKey) REFERENCES FactShipment(ShipmentKey);

ALTER TABLE FactExceptionEvent
ADD CONSTRAINT FK_ExceptionEvent_ExceptionType
    FOREIGN KEY (ExceptionTypeKey) REFERENCES DimExceptionType(ExceptionTypeKey);

ALTER TABLE FactExceptionEvent
ADD CONSTRAINT FK_ExceptionEvent_Date
    FOREIGN KEY (EventDateKey) REFERENCES DimDate(DateKey);

ALTER TABLE FactExceptionEvent
ADD CONSTRAINT FK_ExceptionEvent_Location
    FOREIGN KEY (EventLocationKey) REFERENCES DimLocation(LocationKey);

