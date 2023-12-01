package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

// User represents the User entity
type User struct {
	ID    gocql.UUID `json:"id"`
	Name  string     `json:"name"`
	Roles []Role     `json:"roles"`
}

// Role represents the Role entity
type Role struct {
	ID    gocql.UUID `json:"id"`
	Name  string     `json:"name"`
	Users []User     `json:"users"`
}

var Session *gocql.Session

func main() {
	cluster := gocql.NewCluster("127.0.0.1:9042")
	cluster.Keyspace = "userrole"
	Session, _ = cluster.CreateSession()
	defer Session.Close()

	//Create keyspace and tables if not exists
	err := createSchema()
	if err != nil {
		log.Fatal(err)
	}
	user := User{
		ID:   gocql.TimeUUID(),
		Name: "Nweni Soe",
		Roles: []Role{
			{ID: gocql.TimeUUID(), Name: "Admin"},
			{ID: gocql.TimeUUID(), Name: "Editor"},
			{ID: gocql.TimeUUID(), Name: "User"},
		},
	}

	// Create user and roles
	err = createUser(user)
	if err != nil {
		log.Fatal(err)
	}

	// Fetch user with roles
	fetchedUser, err := getUser(user.ID)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Fetched User: %+v\n", fetchedUser)

	// Update user
	fetchedUser.Name = "Aye Myo Thant"
	err = updateUser(fetchedUser)
	if err != nil {
		log.Fatal(err)
	}

	// Delete user
	err = deleteUser(fetchedUser.ID)
	if err != nil {
		log.Fatal(err)
	}
}

func createSchema() error {
	if err := Session.Query(`
		CREATE KEYSPACE IF NOT EXISTS userRole WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
	`).Exec(); err != nil {
		return err
	}

	if err := Session.Query(`
		CREATE TABLE IF NOT EXISTS userRole.users (
			id UUID PRIMARY KEY,
			name text
		);
	`).Exec(); err != nil {
		return err
	}

	if err := Session.Query(`
		CREATE TABLE IF NOT EXISTS userRole.roles (
			id UUID PRIMARY KEY,
			name text
		);
	`).Exec(); err != nil {
		return err
	}

	if err := Session.Query(`
		CREATE TABLE IF NOT EXISTS userRole.user_roles (
			user_id UUID,
			role_id UUID,
			PRIMARY KEY (user_id, role_id)
		);
	`).Exec(); err != nil {
		return err
	}

	return nil
}

func createUser(user User) error {

	if err := Session.Query(`
		INSERT INTO userRole.users (id, name) VALUES (?, ?);
	`, user.ID, user.Name).Exec(); err != nil {
		return err
	}

	for _, role := range user.Roles {
		if err := Session.Query(`
			INSERT INTO userRole.roles (id, name) VALUES (?, ?);
		`, role.ID, role.Name).Exec(); err != nil {
			return err
		}

		if err := Session.Query(`
			INSERT INTO userRole.user_roles (user_id, role_id) VALUES (?, ?);
		`, user.ID, role.ID).Exec(); err != nil {
			return err
		}
	}

	return nil
}

func getUser(userID gocql.UUID) (User, error) {
	var user User

	err := Session.Query(`
		SELECT id, name FROM userRole.users WHERE id = ?;
	`, userID).Consistency(gocql.One).Scan(&user.ID, &user.Name)

	if err != nil {
		return user, err
	}

	iter := Session.Query(`
		SELECT role_id FROM userRole.user_roles WHERE user_id = ?;
	`, userID).Iter()

	var roleID gocql.UUID
	for iter.Scan(&roleID) {
		var role Role
		err := Session.Query(`
			SELECT id, name FROM userRole.roles WHERE id = ?;
		`, roleID).Consistency(gocql.One).Scan(&role.ID, &role.Name)
		if err != nil {
			return user, err
		}

		user.Roles = append(user.Roles, role)
	}

	if err := iter.Close(); err != nil {
		return user, err
	}

	return user, nil
}

func updateUser(user User) error {
	// Update user details
	if err := Session.Query(`
		UPDATE userRole.users SET name = ? WHERE id = ?;
	`, user.Name, user.ID).Exec(); err != nil {
		return err
	}

	// Delete existing user roles
	if err := Session.Query(`
		DELETE FROM userRole.user_roles WHERE user_id = ?;
	`, user.ID).Exec(); err != nil {
		return err
	}

	// Insert updated user roles
	for _, role := range user.Roles {
		if err := Session.Query(`
			INSERT INTO userRole.user_roles (user_id, role_id) VALUES (?, ?);
		`, user.ID, role.ID).Exec(); err != nil {
			return err
		}
	}

	return nil
}

func deleteUser(userID gocql.UUID) error {
	// Delete user roles
	if err := Session.Query(`
		DELETE FROM userRole.user_roles WHERE user_id = ?;
	`, userID).Exec(); err != nil {
		return err
	}

	// Delete user
	if err := Session.Query(`
		DELETE FROM userRole.users WHERE id = ?;
	`, userID).Exec(); err != nil {
		return err
	}

	return nil
}
