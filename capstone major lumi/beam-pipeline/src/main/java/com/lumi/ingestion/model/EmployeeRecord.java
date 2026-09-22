package com.lumi.ingestion.model;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Typed representation of one employee record as it flows through the pipeline.
 *
 */
@DefaultCoder(SerializableCoder.class)
public class EmployeeRecord implements Serializable {

    private UUID employeeId;
    private String firstName;
    private String lastName;
    private String email;
    private String phoneNumber;
    private LocalDate hireDate;
    private String department;
    private String jobTitle;
    private BigDecimal salary;
    private String currency;
    private String employmentStatus;
    private UUID managerId;
    private Boolean isActive;
    private List<String> skills;
    private String address;             // stored as JSON string for JDBC JSONB binding
    private String emergencyContact;    // stored as JSON string for JDBC JSONB binding

    // encrypted fields (AES-256-GCM, Base64-encoded) 
    private String salaryEncrypted;
    private String phoneNumberEncrypted;
    private String emergencyContactEncrypted;

    // --- metadata fields ---
    private Instant ingestionTimestamp;
    private UUID executionId;
    private Instant sourceCreationTime;

    public EmployeeRecord() {}

    /** Copy-constructor used by EnrichEmployeeFn to avoid mutating the input element. */
    public EmployeeRecord(EmployeeRecord src,
                          Instant ingestionTimestamp,
                          UUID executionId) {
        this.employeeId        = src.employeeId;
        this.firstName         = src.firstName;
        this.lastName          = src.lastName;
        this.email             = src.email;
        this.phoneNumber       = src.phoneNumber;
        this.hireDate          = src.hireDate;
        this.department        = src.department;
        this.jobTitle          = src.jobTitle;
        this.salary            = src.salary;
        this.currency          = src.currency;
        this.employmentStatus  = src.employmentStatus;
        this.managerId         = src.managerId;
        this.isActive          = src.isActive;
        this.skills            = src.skills;
        this.address              = src.address;
        this.emergencyContact     = src.emergencyContact;
        this.salaryEncrypted      = src.salaryEncrypted;
        this.phoneNumberEncrypted = src.phoneNumberEncrypted;
        this.emergencyContactEncrypted = src.emergencyContactEncrypted;
        this.sourceCreationTime   = src.sourceCreationTime;
        this.ingestionTimestamp = ingestionTimestamp;
        this.executionId        = executionId;
    }

    public UUID getEmployeeId() { return employeeId; }
    public void setEmployeeId(UUID employeeId) { this.employeeId = employeeId; }

    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }

    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getPhoneNumber() { return phoneNumber; }
    public void setPhoneNumber(String phoneNumber) { this.phoneNumber = phoneNumber; }

    public LocalDate getHireDate() { return hireDate; }
    public void setHireDate(LocalDate hireDate) { this.hireDate = hireDate; }

    public String getDepartment() { return department; }
    public void setDepartment(String department) { this.department = department; }

    public String getJobTitle() { return jobTitle; }
    public void setJobTitle(String jobTitle) { this.jobTitle = jobTitle; }

    public BigDecimal getSalary() { return salary; }
    public void setSalary(BigDecimal salary) { this.salary = salary; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public String getEmploymentStatus() { return employmentStatus; }
    public void setEmploymentStatus(String employmentStatus) { this.employmentStatus = employmentStatus; }

    public UUID getManagerId() { return managerId; }
    public void setManagerId(UUID managerId) { this.managerId = managerId; }

    public Boolean getIsActive() { return isActive; }
    public void setIsActive(Boolean isActive) { this.isActive = isActive; }

    public List<String> getSkills() { return skills; }
    public void setSkills(List<String> skills) { this.skills = skills; }

    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }

    public String getEmergencyContact() { return emergencyContact; }
    public void setEmergencyContact(String emergencyContact) { this.emergencyContact = emergencyContact; }

    public String getSalaryEncrypted() { return salaryEncrypted; }
    public void setSalaryEncrypted(String salaryEncrypted) { this.salaryEncrypted = salaryEncrypted; }

    public String getPhoneNumberEncrypted() { return phoneNumberEncrypted; }
    public void setPhoneNumberEncrypted(String phoneNumberEncrypted) { this.phoneNumberEncrypted = phoneNumberEncrypted; }

    public String getEmergencyContactEncrypted() { return emergencyContactEncrypted; }
    public void setEmergencyContactEncrypted(String emergencyContactEncrypted) { this.emergencyContactEncrypted = emergencyContactEncrypted; }

    public Instant getIngestionTimestamp() { return ingestionTimestamp; }
    public void setIngestionTimestamp(Instant ingestionTimestamp) { this.ingestionTimestamp = ingestionTimestamp; }

    public UUID getExecutionId() { return executionId; }
    public void setExecutionId(UUID executionId) { this.executionId = executionId; }

    public Instant getSourceCreationTime() { return sourceCreationTime; }
    public void setSourceCreationTime(Instant sourceCreationTime) { this.sourceCreationTime = sourceCreationTime; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EmployeeRecord)) return false;
        EmployeeRecord that = (EmployeeRecord) o;
        return Objects.equals(employeeId, that.employeeId)
            && Objects.equals(firstName, that.firstName)
            && Objects.equals(lastName, that.lastName)
            && Objects.equals(email, that.email)
            && Objects.equals(phoneNumber, that.phoneNumber)
            && Objects.equals(hireDate, that.hireDate)
            && Objects.equals(department, that.department)
            && Objects.equals(jobTitle, that.jobTitle)
            && Objects.equals(salary, that.salary)
            && Objects.equals(currency, that.currency)
            && Objects.equals(employmentStatus, that.employmentStatus)
            && Objects.equals(managerId, that.managerId)
            && Objects.equals(isActive, that.isActive)
            && Objects.equals(skills, that.skills)
            && Objects.equals(address, that.address)
            && Objects.equals(emergencyContact, that.emergencyContact)
            && Objects.equals(salaryEncrypted, that.salaryEncrypted)
            && Objects.equals(phoneNumberEncrypted, that.phoneNumberEncrypted)
            && Objects.equals(emergencyContactEncrypted, that.emergencyContactEncrypted)
            && Objects.equals(ingestionTimestamp, that.ingestionTimestamp)
            && Objects.equals(executionId, that.executionId)
            && Objects.equals(sourceCreationTime, that.sourceCreationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(employeeId, firstName, lastName, email, phoneNumber,
            hireDate, department, jobTitle, salary, currency, employmentStatus,
            managerId, isActive, skills, address, emergencyContact,
            salaryEncrypted, phoneNumberEncrypted, emergencyContactEncrypted,
            ingestionTimestamp, executionId, sourceCreationTime);
    }
}