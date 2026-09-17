package com.igot.cb.community.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;

import java.sql.Timestamp;


/**
 * @author mahesh.vakkund
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "communities")
@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
public class CommunityEntity {
    @Id
    private String communityId;

    @Type(JsonType.class)
    @Column(columnDefinition = "jsonb")
    private JsonNode data;

    private Timestamp createdOn;

    private Timestamp updatedOn;

    private String created_by;

    @Column(name="is_active")
    private boolean isActive;
}
