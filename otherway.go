package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

// User represents the User entity
type Users struct {
	ID    gocql.UUID `json:"id"`
	Name  string     `json:"name"`
	Roles []Role     `json:"roles"`
}

// Role represents the Role entity
type Roles struct {
	ID    gocql.UUID `json:"id"`
	Name  string     `json:"name"`
	Users []User     `json:"users"`
}

var (
	session *gocql.Session
)

func init() {
	// Initialize Cassandra session
	cluster := gocql.NewCluster("127.0.0.1:9042")
	cluster.Keyspace = "your_keyspace_name"
	cluster.Consistency = gocql.Quorum

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
}

func closeSession() {
	// Close the Cassandra session
	if session != nil {
		session.Close()
	}
}

// CRUD operations for User

func createUsers(name string) (gocql.UUID, error) {
	userID := gocql.TimeUUID()
	err := session.Query(`
		INSERT INTO users (id, name)
		VALUES (?, ?)`,
		userID, name).Exec()

	return userID, err
}

func getUsersById(userID gocql.UUID) (User, error) {
	var user User
	err := session.Query(`
		SELECT id, name
		FROM users
		WHERE id = ?`,
		userID).Scan(&user.ID, &user.Name)

	if err != nil {
		return User{}, err
	}

	return user, nil
}

// CRUD operations for Role

func createRoles(name string) (gocql.UUID, error) {
	roleID := gocql.TimeUUID()
	err := session.Query(`
		INSERT INTO roles (id, name)
		VALUES (?, ?)`,
		roleID, name).Exec()

	return roleID, err
}

func getRolesById(roleID gocql.UUID) (Role, error) {
	var role Role
	err := session.Query(`
		SELECT id, name
		FROM roles
		WHERE id = ?`,
		roleID).Scan(&role.ID, &role.Name)

	if err != nil {
		return Role{}, err
	}

	return role, nil
}

// Assign user to role and vice versa

func assignUserToRole(userID, roleID gocql.UUID) error {
	err := session.Query(`
		INSERT INTO user_roles (user_id, role_id)
		VALUES (?, ?)`,
		userID, roleID).Exec()

	return err
}

func assignRoleToUser(userID, roleID gocql.UUID) error {
	err := session.Query(`
		INSERT INTO role_users (role_id, user_id)
		VALUES (?, ?)`,
		roleID, userID).Exec()

	return err
}

// Example usage

func main() {
	defer closeSession()

	// Create a user and a role
	userID, err := createUsers("John Doe")
	if err != nil {
		log.Fatal(err)
	}

	roleID, err := createRoles("Admin")
	if err != nil {
		log.Fatal(err)
	}

	// Assign the user to the role
	err = assignUserToRole(userID, roleID)
	if err != nil {
		log.Fatal(err)
	}

	// Retrieve user and role with their relationships
	user, err := getUsersById(userID)
	if err != nil {
		log.Fatal(err)
	}

	role, err := getRolesById(roleID)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("User: %+v\n", user)
	fmt.Printf("Role: %+v\n", role)
}
