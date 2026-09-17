package com.igot.cb.community.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Timestamp;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "community_category")
@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
public class CommunityCategory {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY) // Auto-increment strategy
  @Column(name = "category_id")
  private Integer categoryId;

  @Column(name = "category_name", nullable = false)
  private String categoryName;

  @Column(name = "description")
  private String description;

  @Column(name = "parent_id")
  private Integer parentId;

  @Column(name = "is_active", nullable = false)
  private Boolean isActive = true; // Default value

  @Column(name = "created_at")
  private Timestamp createdAt;

  @Column(name = "last_updated_at")
  private Timestamp lastUpdatedAt;

  @Column(name = "department_id")
  private String departmentId;

  @Column(name = "count_of_communities")
  private Long countOfCommunities;

}
