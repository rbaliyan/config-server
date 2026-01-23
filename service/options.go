package service

// Option configures the Service.
type Option func(*Service)

// WithAuthorizer sets the authorizer for the service.
func WithAuthorizer(a Authorizer) Option {
	return func(s *Service) {
		s.authorizer = a
	}
}
