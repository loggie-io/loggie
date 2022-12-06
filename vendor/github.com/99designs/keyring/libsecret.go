// +build linux

package keyring

import (
	"encoding/json"
	"errors"

	"github.com/godbus/dbus"
	"github.com/gsterjov/go-libsecret"
)

func init() {
	// silently fail if dbus isn't available
	_, err := dbus.SessionBus()
	if err != nil {
		return
	}

	supportedBackends[SecretServiceBackend] = opener(func(cfg Config) (Keyring, error) {
		if cfg.ServiceName == "" {
			cfg.ServiceName = "secret-service"
		}
		if cfg.LibSecretCollectionName == "" {
			cfg.LibSecretCollectionName = cfg.ServiceName
		}

		service, err := libsecret.NewService()
		if err != nil {
			return &secretsKeyring{}, err
		}

		ring := &secretsKeyring{
			name:    cfg.LibSecretCollectionName,
			service: service,
		}

		return ring, ring.openSecrets()
	})
}

type secretsKeyring struct {
	name       string
	service    *libsecret.Service
	collection *libsecret.Collection
	session    *libsecret.Session
}

type secretsError struct {
	message string
}

func (e *secretsError) Error() string {
	return e.message
}

var errCollectionNotFound = errors.New("The collection does not exist. Please add a key first")

func (k *secretsKeyring) openSecrets() error {
	session, err := k.service.Open()
	if err != nil {
		return err
	}
	k.session = session

	// get the collection if it already exists
	collections, err := k.service.Collections()
	if err != nil {
		return err
	}

	path := libsecret.DBusPath + "/collection/" + k.name

	for _, collection := range collections {
		if string(collection.Path()) == path {
			k.collection = &collection
			return nil
		}
	}

	return nil
}

func (k *secretsKeyring) openCollection() error {
	if err := k.openSecrets(); err != nil {
		return err
	}

	if k.collection == nil {
		return errCollectionNotFound
		// return &secretsError{fmt.Sprintf(
		// 	"The collection %q does not exist. Please add a key first",
		// 	k.name,
		// )}
	}

	return nil
}

func (k *secretsKeyring) Get(key string) (Item, error) {
	if err := k.openCollection(); err != nil {
		if err == errCollectionNotFound {
			return Item{}, ErrKeyNotFound
		}
		return Item{}, err
	}

	items, err := k.collection.SearchItems(key)
	if err != nil {
		return Item{}, err
	}

	if len(items) == 0 {
		return Item{}, ErrKeyNotFound
	}

	// use the first item whenever there are multiples
	// with the same profile name
	item := items[0]

	locked, err := item.Locked()
	if err != nil {
		return Item{}, err
	}

	if locked {
		if err := k.service.Unlock(item); err != nil {
			return Item{}, err
		}
	}

	secret, err := item.GetSecret(k.session)
	if err != nil {
		return Item{}, err
	}

	// pack the secret into the item
	var ret Item
	if err = json.Unmarshal(secret.Value, &ret); err != nil {
		return Item{}, err
	}

	return ret, err
}

// GetMetadata for libsecret returns an error indicating that it's unsupported
// for this backend.
//
// libsecret actually implements a metadata system which we could use, "Secret
// Attributes"; I found no indication in documentation of anything like an
// automatically maintained last-modification timestamp, so to use this we'd
// need to have a SetMetadata API too.  Which we're not yet doing, but feel
// free to contribute patches.
func (k *secretsKeyring) GetMetadata(key string) (Metadata, error) {
	return Metadata{}, ErrMetadataNeedsCredentials
}

func (k *secretsKeyring) Set(item Item) error {
	err := k.openSecrets()
	if err != nil {
		return err
	}

	// create the collection if it doesn't already exist
	if k.collection == nil {
		collection, err := k.service.CreateCollection(k.name)
		if err != nil {
			return err
		}

		k.collection = collection
	}

	if err := k.ensureCollectionUnlocked(); err != nil {
		return err
	}

	// create the new item
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}

	secret := libsecret.NewSecret(k.session, []byte{}, data, "application/json")

	if _, err := k.collection.CreateItem(item.Key, secret, true); err != nil {
		return err
	}

	return nil
}

func (k *secretsKeyring) Remove(key string) error {
	if err := k.openCollection(); err != nil {
		if err == errCollectionNotFound {
			return ErrKeyNotFound
		}
		return err
	}

	items, err := k.collection.SearchItems(key)
	if err != nil {
		return err
	}

	// nothing to delete
	if len(items) == 0 {
		return nil
	}

	// we dont want to delete more than one anyway
	// so just get the first item found
	item := items[0]

	locked, err := item.Locked()
	if err != nil {
		return err
	}

	if locked {
		if err := k.service.Unlock(item); err != nil {
			return err
		}
	}

	if err := item.Delete(); err != nil {
		return err
	}

	return nil
}

func (k *secretsKeyring) Keys() ([]string, error) {
	if err := k.openCollection(); err != nil {
		if err == errCollectionNotFound {
			return []string{}, nil
		}
		return nil, err
	}
	if err := k.ensureCollectionUnlocked(); err != nil {
		return nil, err
	}
	items, err := k.collection.Items()
	if err != nil {
		return nil, err
	}
	keys := []string{}
	for _, item := range items {
		label, err := item.Label()
		if err == nil {
			keys = append(keys, label)
		} else {
			// err is being silently ignored here, not sure if that's good or bad
		}
	}
	return keys, nil
}

// deleteCollection deletes the keyring's collection if it exists. This is mainly to support testing.
func (k *secretsKeyring) deleteCollection() error {
	if err := k.openCollection(); err != nil {
		return err
	}
	return k.collection.Delete()
}

// unlock the collection if it's locked
func (k *secretsKeyring) ensureCollectionUnlocked() error {
	locked, err := k.collection.Locked()
	if err != nil {
		return err
	}
	if !locked {
		return nil
	}
	return k.service.Unlock(k.collection)
}
