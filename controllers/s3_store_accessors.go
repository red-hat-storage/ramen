// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"github.com/go-logr/logr"
	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

type s3StoreAccessor struct {
	ObjectStorer
	ramen.S3StoreProfile
}

func s3StoreAccessorsGet(
	s3ProfileNames []string,
	objectStorerGet func(string) (ObjectStorer, ramen.S3StoreProfile, error),
	log logr.Logger,
) []s3StoreAccessor {
	s3StoreAccessors := make([]s3StoreAccessor, 0, len(s3ProfileNames))

	for _, s3ProfileName := range s3ProfileNames {
		if s3ProfileName == NoS3StoreAvailable {
			log.Info("Kube object protection store dummy")

			continue
		}

		objectStorer, s3StoreProfile, err := objectStorerGet(s3ProfileName)
		if err != nil {
			log.Error(err, "Kube object protection store inaccessible", "name", s3ProfileName)

			return nil
		}

		s3StoreAccessors = append(s3StoreAccessors, s3StoreAccessor{
			objectStorer,
			s3StoreProfile,
		})
	}

	return s3StoreAccessors
}
